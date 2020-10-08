package com.instaclustr.esop.impl.backup;

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
import java.nio.file.Paths;

import com.instaclustr.kubernetes.KubernetesHelper;

@Target({TYPE, PARAMETER})
@Retention(RUNTIME)
@Constraint(validatedBy = {
    ValidBackupCommitLogsOperationRequest.BackupCommitLogsOperationRequestValidator.class,
})
public @interface ValidBackupCommitLogsOperationRequest {

    String message() default "{com.instaclustr.aesop.impl.backup.ValidBackupCommitLogsOperationRequest.BackupCommitLogsOperationRequestValidator.message}";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

    final class BackupCommitLogsOperationRequestValidator implements ConstraintValidator<ValidBackupCommitLogsOperationRequest, BackupCommitLogsOperationRequest> {

        @Override
        public boolean isValid(final BackupCommitLogsOperationRequest value, final ConstraintValidatorContext context) {

            context.disableDefaultConstraintViolation();

            if (value.cassandraDirectory == null || value.cassandraDirectory.toFile().getAbsolutePath().equals("/")) {
                value.cassandraDirectory = Paths.get("/var/lib/cassandra");
            }

            if (!Files.exists(value.cassandraDirectory)) {
                context.buildConstraintViolationWithTemplate(String.format("cassandraDirectory %s does not exist", value.cassandraDirectory)).addConstraintViolation();
                return false;
            }

            if (KubernetesHelper.isRunningInKubernetes() && value.resolveKubernetesSecretName() == null) {
                context.buildConstraintViolationWithTemplate("This code is running in Kubernetes but there is not 'k8sSecretName' field set on backup request!").addConstraintViolation();
                return false;
            }

            return true;
        }
    }
}
