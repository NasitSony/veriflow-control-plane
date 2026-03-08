package k8s

import (
	"context"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func CreateJob(client *kubernetes.Clientset, namespace string, jobName string, image string, command []string, jobID, runID string) error {

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: jobName,
			Labels: map[string]string{
				"veriflow/run_id": runID, // pass runID as string
				"veriflow/job_id": jobID, // pass jobID as string
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: int32Ptr(0), // IMPORTANT: we handle retries
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:    "runner",
							Image:   image,
							Command: command,
						},
					},
				},
			},
		},
	}

	_, err := client.BatchV1().Jobs(namespace).Create(context.Background(), job, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create k8s job: %w", err)
	}

	return nil
}

func int32Ptr(i int32) *int32 {
	return &i
}
