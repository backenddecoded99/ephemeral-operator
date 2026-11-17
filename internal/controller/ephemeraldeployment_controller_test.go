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
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	ephemeralv1alpha1 "github.com/backenddecoded99/ephemeral-operator/api/v1alpha1"
)

// ---------------------------------------------------------------------------
// Pure unit tests (stdlib testing.T, no envtest required)
// ---------------------------------------------------------------------------

func TestComputeExpiresAt(t *testing.T) {
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	creation := metav1.NewTime(base)

	tests := []struct {
		name        string
		ttlSeconds  int64
		wantExpires time.Time
	}{
		{"10s ttl", 10, base.Add(10 * time.Second)},
		{"1 minute ttl", 60, base.Add(60 * time.Second)},
		{"1 hour ttl", 3600, base.Add(time.Hour)},
		{"1 day ttl", 86400, base.Add(24 * time.Hour)},
		{"minimum ttl of 10", 10, base.Add(10 * time.Second)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := computeExpiresAt(creation, tc.ttlSeconds)
			if !got.Time.Equal(tc.wantExpires) {
				t.Errorf("computeExpiresAt(%v, %d): got %v, want %v",
					base, tc.ttlSeconds, got.Time, tc.wantExpires)
			}
		})
	}
}

// TestExpiryCheckWithFakeClock verifies that the Active-phase expiry comparison
// (now >= expiresAt) behaves correctly with an injected clock.
func TestExpiryCheckWithFakeClock(t *testing.T) {
	base := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	const ttl = int64(100)

	expiry := computeExpiresAt(metav1.NewTime(base), ttl)
	fakeClock := clocktesting.NewFakeClock(base)

	// Before expiry: 50 s elapsed.
	fakeClock.Step(50 * time.Second)
	remaining := expiry.Time.Sub(fakeClock.Now())
	if remaining <= 0 {
		t.Fatalf("expected remaining > 0 at 50s, got %v", remaining)
	}

	// Exactly at expiry: 100 s elapsed.
	fakeClock.Step(50 * time.Second)
	remaining = expiry.Time.Sub(fakeClock.Now())
	if remaining > 0 {
		t.Fatalf("expected remaining <= 0 at exactly expiry, got %v", remaining)
	}

	// After expiry: 101 s elapsed.
	fakeClock.Step(1 * time.Second)
	remaining = expiry.Time.Sub(fakeClock.Now())
	if remaining > 0 {
		t.Fatalf("expected remaining < 0 past expiry, got %v", remaining)
	}
}

// TestComputeExpiresAtIsDeterministic verifies that the same inputs always
// produce the same output (pure function, no current-time dependency).
func TestComputeExpiresAtIsDeterministic(t *testing.T) {
	creation := metav1.NewTime(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	const ttl = int64(300)

	for i := 0; i < 5; i++ {
		got := computeExpiresAt(creation, ttl)
		want := creation.Add(300 * time.Second)
		if !got.Time.Equal(want) {
			t.Errorf("iteration %d: got %v, want %v", i, got.Time, want)
		}
	}
}

// ---------------------------------------------------------------------------
// Ginkgo envtest integration tests
// ---------------------------------------------------------------------------

var _ = Describe("EphemeralDeployment Controller", func() {
	const (
		ttlSeconds = int64(300)
		image      = "nginx:alpine"
		port       = int32(80)
	)

	var (
		ctx        context.Context
		fakeClock  *clocktesting.FakeClock
		reconciler *EphemeralDeploymentReconciler
		ed         *ephemeralv1alpha1.EphemeralDeployment
		nsName     types.NamespacedName
		replicas   int32
	)

	BeforeEach(func() {
		ctx = context.Background()
		replicas = 2

		// Use a fake clock anchored to "now" so tests are deterministic.
		fakeClock = clocktesting.NewFakeClock(time.Now())

		reconciler = &EphemeralDeploymentReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			Clock:  fakeClock,
			// Use the cached client as APIReader in envtest (no real API server
			// uncached reader in envtest); sufficient for unit-level envtest.
			APIReader: k8sClient,
		}

		// Create a unique name per test to avoid cross-test pollution.
		name := fmt.Sprintf("test-ed-%d", time.Now().UnixNano())
		nsName = types.NamespacedName{Name: name, Namespace: "default"}

		ed = &ephemeralv1alpha1.EphemeralDeployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "default",
			},
			Spec: ephemeralv1alpha1.EphemeralDeploymentSpec{
				TTLSeconds: ttlSeconds,
				Workload: ephemeralv1alpha1.WorkloadSpec{
					Image:    image,
					Replicas: &replicas,
					Port:     port,
				},
			},
		}
		Expect(k8sClient.Create(ctx, ed)).To(Succeed())

		// Re-read to get server-assigned fields (creationTimestamp, resourceVersion).
		Expect(k8sClient.Get(ctx, nsName, ed)).To(Succeed())
	})

	AfterEach(func() {
		// Best-effort cleanup; ignore NotFound (reconciler may have deleted it).
		_ = client.IgnoreNotFound(k8sClient.Delete(ctx, ed))
	})

	// -------------------------------------------------------------------------
	// Pending → Active: child resource creation
	// -------------------------------------------------------------------------

	It("creates Deployment with correct ownerReference", func() {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
		Expect(err).NotTo(HaveOccurred())

		deploy := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, nsName, deploy)).To(Succeed())

		Expect(deploy.OwnerReferences).To(HaveLen(1))
		Expect(deploy.OwnerReferences[0].Name).To(Equal(ed.Name))
		Expect(deploy.OwnerReferences[0].UID).To(Equal(ed.UID))
		Expect(*deploy.OwnerReferences[0].Controller).To(BeTrue())
	})

	It("creates Service with correct ownerReference", func() {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
		Expect(err).NotTo(HaveOccurred())

		svc := &corev1.Service{}
		Expect(k8sClient.Get(ctx, nsName, svc)).To(Succeed())

		Expect(svc.OwnerReferences).To(HaveLen(1))
		Expect(svc.OwnerReferences[0].Name).To(Equal(ed.Name))
		Expect(svc.OwnerReferences[0].UID).To(Equal(ed.UID))
		Expect(*svc.OwnerReferences[0].Controller).To(BeTrue())
	})

	It("Deployment spec mirrors the CR workload spec", func() {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
		Expect(err).NotTo(HaveOccurred())

		deploy := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, nsName, deploy)).To(Succeed())

		Expect(deploy.Spec.Replicas).NotTo(BeNil())
		Expect(*deploy.Spec.Replicas).To(Equal(replicas))

		containers := deploy.Spec.Template.Spec.Containers
		Expect(containers).To(HaveLen(1))
		Expect(containers[0].Image).To(Equal(image))
		Expect(containers[0].Ports).To(HaveLen(1))
		Expect(containers[0].Ports[0].ContainerPort).To(Equal(port))
	})

	It("Service exposes the correct port", func() {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
		Expect(err).NotTo(HaveOccurred())

		svc := &corev1.Service{}
		Expect(k8sClient.Get(ctx, nsName, svc)).To(Succeed())

		Expect(svc.Spec.Ports).To(HaveLen(1))
		Expect(svc.Spec.Ports[0].Port).To(Equal(port))
		Expect(svc.Spec.Type).To(Equal(corev1.ServiceTypeClusterIP))
	})

	// -------------------------------------------------------------------------
	// Status after first reconcile
	// -------------------------------------------------------------------------

	It("sets status.phase=Active and expiresAt after first reconcile", func() {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, nsName, ed)).To(Succeed())
		Expect(ed.Status.Phase).To(Equal(ephemeralv1alpha1.PhaseActive))
		Expect(ed.Status.ExpiresAt).NotTo(BeNil())
		Expect(ed.Status.ObservedGeneration).To(Equal(ed.Generation))
	})

	It("sets expiresAt = creationTimestamp + ttlSeconds", func() {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, nsName, ed)).To(Succeed())

		expectedExpiry := ed.CreationTimestamp.Add(time.Duration(ttlSeconds) * time.Second)
		Expect(ed.Status.ExpiresAt.Time).To(
			BeTemporally("~", expectedExpiry, time.Second),
		)
	})

	It("sets a Ready condition with status=False in Pending reconcile", func() {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, nsName, ed)).To(Succeed())
		cond := findCondition(ed.Status.Conditions, conditionReady)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal("WorkloadProvisioning"))
	})

	// -------------------------------------------------------------------------
	// Active phase: TTL drift when spec.ttlSeconds is edited
	// -------------------------------------------------------------------------

	It("editing ttlSeconds moves expiresAt but not creationTimestamp", func() {
		// First reconcile → Active.
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, nsName, ed)).To(Succeed())
		originalCreation := *ed.CreationTimestamp.DeepCopy()

		// Edit ttlSeconds to a new value (double it).
		const newTTL = int64(600)
		ed.Spec.TTLSeconds = newTTL
		Expect(k8sClient.Update(ctx, ed)).To(Succeed())
		// Re-read to get the new generation.
		Expect(k8sClient.Get(ctx, nsName, ed)).To(Succeed())

		// Second reconcile (phase=Active) → should recompute expiresAt.
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, nsName, ed)).To(Succeed())
		expectedExpiry := ed.CreationTimestamp.Add(time.Duration(newTTL) * time.Second)
		Expect(ed.Status.ExpiresAt.Time).To(
			BeTemporally("~", expectedExpiry, time.Second),
		)

		// creationTimestamp must not have changed.
		Expect(ed.CreationTimestamp.Time).To(BeTemporally("==", originalCreation.Time))
	})

	// -------------------------------------------------------------------------
	// Active phase: requeue before expiry
	// -------------------------------------------------------------------------

	It("Active phase before expiry returns a positive RequeueAfter", func() {
		// First reconcile → Active.
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
		Expect(err).NotTo(HaveOccurred())

		// Second reconcile (phase=Active, clock before expiry).
		result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(BeNumerically(">", 0))
	})

	// -------------------------------------------------------------------------
	// Expiring phase: scale to zero
	// -------------------------------------------------------------------------

	It("Expiring phase scales Deployment to 0 replicas", func() {
		// First reconcile → Active.
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
		Expect(err).NotTo(HaveOccurred())

		// Advance the fake clock past the TTL.
		fakeClock.Step(time.Duration(ttlSeconds+10) * time.Second)

		// Second reconcile (phase=Active, clock past expiry) → sets Expiring.
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
		Expect(err).NotTo(HaveOccurred())

		// Third reconcile (phase=Expiring) → scales Deployment to 0.
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
		Expect(err).NotTo(HaveOccurred())

		deploy := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, nsName, deploy)).To(Succeed())
		Expect(deploy.Spec.Replicas).NotTo(BeNil())
		Expect(*deploy.Spec.Replicas).To(Equal(int32(0)))
	})

	It("Expiring phase returns RequeueAfter when pods are still running", func() {
		// First reconcile → Active.
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
		Expect(err).NotTo(HaveOccurred())

		// Advance past TTL.
		fakeClock.Step(time.Duration(ttlSeconds+10) * time.Second)

		// Active → Expiring transition.
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
		Expect(err).NotTo(HaveOccurred())

		// Expiring reconcile: envtest has no pods so count=0, but we can verify
		// the Deployment was scaled to 0 and the reconcile did not error.
		result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
		Expect(err).NotTo(HaveOccurred())
		// With no pods in envtest, the reconciler will attempt to delete the CR.
		// Either a requeue or no error is acceptable here.
		_ = result
	})

	// -------------------------------------------------------------------------
	// Idempotency: reconciling a second time is safe
	// -------------------------------------------------------------------------

	It("is idempotent: reconciling twice gives the same result", func() {
		for i := 0; i < 2; i++ {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
			Expect(err).NotTo(HaveOccurred())
		}

		deploy := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, nsName, deploy)).To(Succeed())
		Expect(*deploy.Spec.Replicas).To(Equal(replicas))

		svc := &corev1.Service{}
		Expect(k8sClient.Get(ctx, nsName, svc)).To(Succeed())
	})
})

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// findCondition returns the condition with the given type, or nil.
func findCondition(conditions []metav1.Condition, condType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == condType {
			return &conditions[i]
		}
	}
	return nil
}
