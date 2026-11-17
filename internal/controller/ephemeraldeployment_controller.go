/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"reflect"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	ephemeralv1alpha1 "github.com/backenddecoded99/ephemeral-operator/api/v1alpha1"
)

const (
	// conditionReady is the type of the Ready condition on EphemeralDeployment.
	conditionReady = "Ready"

	// drainTimeout is the maximum time to wait for pods to drain in the Expiring
	// phase before forcibly deleting the CR.
	drainTimeout = 5 * time.Minute

	// requeueExpiring is how often to re-check for remaining pods in Expiring phase.
	requeueExpiring = 5 * time.Second
)

// EphemeralDeploymentReconciler reconciles a EphemeralDeployment object.
type EphemeralDeploymentReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// Clock is used for all time operations. Set to clock.RealClock{} in
	// production; inject a fake clock (k8s.io/utils/clock/testing.FakeClock)
	// in tests to control time deterministically.
	Clock clock.PassiveClock
	// APIReader is an uncached client.Reader that reads directly from the API
	// server. Used in the Expiring phase to avoid stale pod counts from the
	// informer cache.
	APIReader client.Reader
}

// +kubebuilder:rbac:groups=ephemeral.backenddecoded99.github.io,resources=ephemeraldeployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ephemeral.backenddecoded99.github.io,resources=ephemeraldeployments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ephemeral.backenddecoded99.github.io,resources=ephemeraldeployments/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list

// Reconcile moves the current state of the EphemeralDeployment toward the desired state.
func (r *EphemeralDeploymentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Ensure a clock is always available (defensive default for tests that
	// construct the reconciler without setting Clock).
	if r.Clock == nil {
		r.Clock = clock.RealClock{}
	}

	// 1. Fetch the CR. NotFound means it was already deleted; nothing to do.
	ed := &ephemeralv1alpha1.EphemeralDeployment{}
	if err := r.Get(ctx, req.NamespacedName, ed); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// 2. If deletion is in progress, do nothing here. Phase B adds finalizer
	//    cleanup in this branch.
	if !ed.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// 3. Snapshot status before any mutations so we can detect changes.
	statusBefore := ed.Status.DeepCopy()

	// 4. Drive the state machine.
	result, err := r.reconcilePhase(ctx, ed)

	// 5. Always stamp observedGeneration.
	ed.Status.ObservedGeneration = ed.Generation

	// 6. Write status only when something actually changed.
	if !reflect.DeepEqual(*statusBefore, ed.Status) {
		if updateErr := r.Status().Update(ctx, ed); updateErr != nil {
			log.Error(updateErr, "Failed to update status")
			return ctrl.Result{}, updateErr
		}
	}

	return result, err
}

// reconcilePhase implements the per-phase state machine. It mutates ed.Status
// but does NOT call Status().Update — the caller handles that.
func (r *EphemeralDeploymentReconciler) reconcilePhase(
	ctx context.Context,
	ed *ephemeralv1alpha1.EphemeralDeployment,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	switch ed.Status.Phase {
	case "", ephemeralv1alpha1.PhasePending:
		return r.reconcilePending(ctx, ed)

	case ephemeralv1alpha1.PhaseActive:
		return r.reconcileActive(ctx, ed)

	case ephemeralv1alpha1.PhaseExpiring:
		return r.reconcileExpiring(ctx, ed)

	default:
		log.Info("Unrecognised phase; treating as Pending", "phase", ed.Status.Phase)
		return r.reconcilePending(ctx, ed)
	}
}

// reconcilePending creates the Deployment and Service, sets expiresAt, and
// transitions to Active.
func (r *EphemeralDeploymentReconciler) reconcilePending(
	ctx context.Context,
	ed *ephemeralv1alpha1.EphemeralDeployment,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Reconciling Pending phase")

	if err := r.ensureDeployment(ctx, ed); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensureService(ctx, ed); err != nil {
		return ctrl.Result{}, err
	}

	ed.Status.ExpiresAt = ptr(computeExpiresAt(ed.CreationTimestamp, ed.Spec.TTLSeconds))
	ed.Status.Phase = ephemeralv1alpha1.PhaseActive

	apimeta.SetStatusCondition(&ed.Status.Conditions, metav1.Condition{
		Type:               conditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             "WorkloadProvisioning",
		Message:            "Deployment and Service have been created; waiting for TTL to expire",
		ObservedGeneration: ed.Generation,
	})

	log.Info("Transitioned to Active", "expiresAt", ed.Status.ExpiresAt)
	return ctrl.Result{}, nil
}

// reconcileActive returns a RequeueAfter for the remaining TTL, or transitions
// to Expiring when the TTL has elapsed. It also recomputes expiresAt if
// spec.ttlSeconds changed since the last reconcile.
func (r *EphemeralDeploymentReconciler) reconcileActive(
	ctx context.Context,
	ed *ephemeralv1alpha1.EphemeralDeployment,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Recompute expiresAt in case spec.ttlSeconds was edited.
	// creationTimestamp is immutable so this is always safe to recompute.
	computed := computeExpiresAt(ed.CreationTimestamp, ed.Spec.TTLSeconds)
	if ed.Status.ExpiresAt == nil || !ed.Status.ExpiresAt.Time.Equal(computed.Time) {
		log.Info("Updating expiresAt due to ttlSeconds change",
			"old", ed.Status.ExpiresAt,
			"new", computed)
		ed.Status.ExpiresAt = ptr(computed)
	}

	now := r.Clock.Now()
	remaining := ed.Status.ExpiresAt.Time.Sub(now)

	if remaining > 0 {
		log.Info("Active: TTL not yet elapsed", "remaining", remaining)
		return ctrl.Result{RequeueAfter: remaining}, nil
	}

	log.Info("Active: TTL elapsed, transitioning to Expiring")
	ed.Status.Phase = ephemeralv1alpha1.PhaseExpiring

	apimeta.SetStatusCondition(&ed.Status.Conditions, metav1.Condition{
		Type:               conditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             "TTLExpired",
		Message:            "TTL has elapsed; draining workload",
		ObservedGeneration: ed.Generation,
	})

	return ctrl.Result{}, nil
}

// reconcileExpiring scales the Deployment to zero, waits for pods to drain
// (capped at drainTimeout), then deletes the CR.
func (r *EphemeralDeploymentReconciler) reconcileExpiring(
	ctx context.Context,
	ed *ephemeralv1alpha1.EphemeralDeployment,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Scale the Deployment to zero (idempotent via patch).
	if err := r.scaleDeploymentToZero(ctx, ed); err != nil {
		return ctrl.Result{}, err
	}

	// Determine how long we have been in the Expiring phase.
	// We use expiresAt as the anchor: draining started at TTL expiry.
	var drainStart time.Time
	if ed.Status.ExpiresAt != nil {
		drainStart = ed.Status.ExpiresAt.Time
	} else {
		// Defensive fallback: use creationTimestamp + ttlSeconds.
		drainStart = computeExpiresAt(ed.CreationTimestamp, ed.Spec.TTLSeconds).Time
	}
	drainDeadline := drainStart.Add(drainTimeout)
	now := r.Clock.Now()

	// Count running pods via the uncached API reader.
	podCount, err := r.countRemainingPods(ctx, ed)
	if err != nil {
		return ctrl.Result{}, err
	}

	if podCount > 0 && now.Before(drainDeadline) {
		log.Info("Expiring: pods still running; requeueing",
			"podCount", podCount,
			"drainDeadline", drainDeadline)
		return ctrl.Result{RequeueAfter: requeueExpiring}, nil
	}

	if podCount > 0 {
		log.Info("Expiring: drain timeout reached; deleting CR despite remaining pods",
			"podCount", podCount)
	} else {
		log.Info("Expiring: all pods terminated; deleting CR")
	}

	// Delete the CR. Owner-reference GC will remove the Deployment and Service.
	// No status write before Delete per spec.
	if err := r.Delete(ctx, ed); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// scaleDeploymentToZero patches the owned Deployment's replicas to 0.
// It is idempotent: if already 0 or the Deployment is not found, it returns nil.
func (r *EphemeralDeploymentReconciler) scaleDeploymentToZero(
	ctx context.Context,
	ed *ephemeralv1alpha1.EphemeralDeployment,
) error {
	log := logf.FromContext(ctx)

	deploy := &appsv1.Deployment{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(ed), deploy); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	if deploy.Spec.Replicas != nil && *deploy.Spec.Replicas == 0 {
		return nil // already scaled to zero
	}

	zero := int32(0)
	patch := client.MergeFrom(deploy.DeepCopy())
	deploy.Spec.Replicas = &zero
	if err := r.Patch(ctx, deploy, patch); err != nil {
		log.Error(err, "Failed to scale Deployment to zero")
		return err
	}

	log.Info("Scaled Deployment to zero")
	return nil
}

// countRemainingPods lists Pods in the CR namespace matching the standard
// label selector. It uses the uncached APIReader to avoid stale counts.
func (r *EphemeralDeploymentReconciler) countRemainingPods(
	ctx context.Context,
	ed *ephemeralv1alpha1.EphemeralDeployment,
) (int, error) {
	reader := r.APIReader
	if reader == nil {
		// Fallback for tests that don't wire up APIReader.
		reader = r.Client
	}

	podList := &corev1.PodList{}
	if err := reader.List(ctx, podList,
		client.InNamespace(ed.Namespace),
		client.MatchingLabels(labelsForED(ed.Name)),
	); err != nil {
		return 0, err
	}
	return len(podList.Items), nil
}

// ensureDeployment creates or updates the Deployment that runs the workload.
func (r *EphemeralDeploymentReconciler) ensureDeployment(
	ctx context.Context,
	ed *ephemeralv1alpha1.EphemeralDeployment,
) error {
	log := logf.FromContext(ctx)

	labels := labelsForED(ed.Name)
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ed.Name,
			Namespace: ed.Namespace,
		},
	}

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, deploy, func() error {
		deploy.Labels = labels

		replicas := int32(1)
		if ed.Spec.Workload.Replicas != nil {
			replicas = *ed.Spec.Workload.Replicas
		}
		deploy.Spec.Replicas = &replicas
		deploy.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
		deploy.Spec.Template = corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: labels},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  ed.Name,
						Image: ed.Spec.Workload.Image,
						Ports: containersPortsForED(ed.Spec.Workload.Port),
					},
				},
			},
		}

		return controllerutil.SetControllerReference(ed, deploy, r.Scheme)
	})
	if err != nil {
		log.Error(err, "Failed to create or update Deployment")
		return err
	}

	log.Info("Deployment reconciled", "operation", op)
	return nil
}

// ensureService creates or updates the ClusterIP Service that exposes the workload.
func (r *EphemeralDeploymentReconciler) ensureService(
	ctx context.Context,
	ed *ephemeralv1alpha1.EphemeralDeployment,
) error {
	log := logf.FromContext(ctx)

	labels := labelsForED(ed.Name)
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ed.Name,
			Namespace: ed.Namespace,
		},
	}

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = labels
		svc.Spec.Selector = labels
		svc.Spec.Type = corev1.ServiceTypeClusterIP

		if ed.Spec.Workload.Port > 0 {
			svc.Spec.Ports = []corev1.ServicePort{
				{
					Name:     "http",
					Port:     ed.Spec.Workload.Port,
					Protocol: corev1.ProtocolTCP,
				},
			}
		}

		return controllerutil.SetControllerReference(ed, svc, r.Scheme)
	})
	if err != nil {
		log.Error(err, "Failed to create or update Service")
		return err
	}

	log.Info("Service reconciled", "operation", op)
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *EphemeralDeploymentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Wire up the uncached API reader from the manager.
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	// Wire up the clock if not already set.
	if r.Clock == nil {
		r.Clock = clock.RealClock{}
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&ephemeralv1alpha1.EphemeralDeployment{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Named("ephemeraldeployment").
		Complete(r)
}

// computeExpiresAt returns creation + ttlSeconds as a metav1.Time.
// This is a pure function: same inputs always give the same output.
func computeExpiresAt(creation metav1.Time, ttlSeconds int64) metav1.Time {
	return metav1.NewTime(creation.Add(time.Duration(ttlSeconds) * time.Second))
}

// labelsForED returns the standard label set for child resources of an
// EphemeralDeployment.
func labelsForED(name string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       name,
		"app.kubernetes.io/managed-by": "ephemeral-operator",
	}
}

// containersPortsForED converts the spec port to a ContainerPort slice.
// Returns nil if port is 0 (not specified).
func containersPortsForED(port int32) []corev1.ContainerPort {
	if port == 0 {
		return nil
	}
	return []corev1.ContainerPort{{ContainerPort: port, Protocol: corev1.ProtocolTCP}}
}

// ptr is a convenience helper that returns a pointer to a metav1.Time value.
func ptr(t metav1.Time) *metav1.Time {
	return &t
}
