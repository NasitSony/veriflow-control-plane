package k8s

import (
	"context"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func CreateJob(
	client kubernetes.Interface,
	namespace string,
	jobName string,
	image string,
	command []string,
	env map[string]string,
	jobID string,
	runID string,
) error {
	var envVars []corev1.EnvVar
	for k, v := range env {
		envVars = append(envVars, corev1.EnvVar{
			Name:  k,
			Value: v,
		})
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: jobName,
			Labels: map[string]string{
				"job_id": jobID,
				"run_id": runID,
			},
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"job_id": jobID,
						"run_id": runID,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Volumes: []corev1.Volume{
						{
							Name: "status-vol",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/tmp/veriflow-status",
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:    "runner",
							Image:   image,
							Command: command,
							Env:     envVars,
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "status-vol",
									MountPath: "/status",
								},
							},
						},
					},
				},
			},
		},
	}

	_, err := client.BatchV1().Jobs(namespace).Create(context.Background(), job, metav1.CreateOptions{})
	return err
}
