package com.instaclustr.cassandra.backup.impl.backup;

import static java.lang.String.format;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.nio.file.Files;

import com.instaclustr.kubernetes.KubernetesHelper;

@Target({TYPE, PARAMETER})
@Retention(RUNTIME)
@Constraint(validatedBy = {
    ValidBackupOperationRequest.BackupOperationRequestValidator.class,
})
public @interface ValidBackupOperationRequest {

    String message() default "{com.instaclustr.cassandra.backup.impl.backup.ValidBackupOperationRequest.BackupOperationRequestValidator.message}";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

    final class BackupOperationRequestValidator implements ConstraintValidator<ValidBackupOperationRequest, BackupOperationRequest> {

        @Override
        public boolean isValid(final BackupOperationRequest value, final ConstraintValidatorContext context) {

            context.disableDefaultConstraintViolation();

            if (!Files.exists(value.sharedContainerPath)) {
                context.buildConstraintViolationWithTemplate(format("sharedContainerPath %s does not exist", value.sharedContainerPath)).addConstraintViolation();
                return false;
            }

            if (!Files.exists(value.cassandraDirectory)) {
                context.buildConstraintViolationWithTemplate(format("cassandraDirectory %s does not exist", value.cassandraDirectory)).addConstraintViolation();
                return false;
            }

            if ((KubernetesHelper.isRunningInKubernetes() || KubernetesHelper.isRunningAsClient()) && value.k8sBackupSecretName == null) {
                context.buildConstraintViolationWithTemplate("This code is running in Kubernetes or as a Kubernetes client "
                                                                 + "but there is not 'k8sSecretName' field set on backup request!").addConstraintViolation();
                return false;
            }

            return true;
        }
    }
}
