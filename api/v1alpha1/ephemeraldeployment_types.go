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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EphemeralDeploymentPhase describes the lifecycle phase.
// +kubebuilder:validation:Enum=Pending;Active;Expiring;Terminating
type EphemeralDeploymentPhase string

const (
	PhasePending     EphemeralDeploymentPhase = "Pending"
	PhaseActive      EphemeralDeploymentPhase = "Active"
	PhaseExpiring    EphemeralDeploymentPhase = "Expiring"
	PhaseTerminating EphemeralDeploymentPhase = "Terminating"
)

// WorkloadSpec is the minimal workload the operator runs.
type WorkloadSpec struct {
	// Container image to run.
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`

	// Number of replicas.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// Container port to expose.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +optional
	Port int32 `json:"port,omitempty"`
}

// EphemeralDeploymentSpec defines the desired state.
type EphemeralDeploymentSpec struct {
	// Seconds after creation before the environment is removed.
	// +kubebuilder:validation:Minimum=10
	// +kubebuilder:validation:Required
	TTLSeconds int64 `json:"ttlSeconds"`

	// Workload to run for the lifetime of this object.
	// +kubebuilder:validation:Required
	Workload WorkloadSpec `json:"workload"`
}

// EphemeralDeploymentStatus defines the observed state.
type EphemeralDeploymentStatus struct {
	// Current lifecycle phase.
	// +optional
	Phase EphemeralDeploymentPhase `json:"phase,omitempty"`

	// When the environment expires: creationTimestamp + ttlSeconds.
	// +optional
	ExpiresAt *metav1.Time `json:"expiresAt,omitempty"`

	// The spec generation these conditions reflect.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Latest observations of the object's state.
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Expires At",type="string",JSONPath=".status.expiresAt"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// EphemeralDeployment is the Schema for the ephemeraldeployments API.
type EphemeralDeployment struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EphemeralDeploymentSpec   `json:"spec,omitempty"`
	Status EphemeralDeploymentStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// EphemeralDeploymentList contains a list of EphemeralDeployment.
type EphemeralDeploymentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EphemeralDeployment `json:"items"`
}

func init() {
	SchemeBuilder.Register(&EphemeralDeployment{}, &EphemeralDeploymentList{})
}
