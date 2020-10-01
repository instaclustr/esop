package com.instaclustr.cassandra.backup.impl.backup;

import static com.instaclustr.kubernetes.KubernetesHelper.isRunningAsClient;
import static com.instaclustr.kubernetes.KubernetesHelper.isRunningInKubernetes;
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
import java.nio.file.Paths;

import com.instaclustr.cassandra.backup.impl.DatabaseEntities;

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

            if (value.cassandraDirectory == null || value.cassandraDirectory.toFile().getAbsolutePath().equals("/")) {
                value.cassandraDirectory = Paths.get("/var/lib/cassandra");
            }

            if (!Files.exists(value.cassandraDirectory)) {
                context.buildConstraintViolationWithTemplate(format("cassandraDirectory %s does not exist", value.cassandraDirectory)).addConstraintViolation();
                return false;
            }

            if ((isRunningInKubernetes() || isRunningAsClient())) {

                if (value.resolveKubernetesSecretName() == null) {
                    context.buildConstraintViolationWithTemplate("This code is running in Kubernetes or as a Kubernetes client "
                                                                     + "but it is not possible to resolve k8s secret name for backups!").addConstraintViolation();

                    return false;
                }

                if (value.resolveKubernetesNamespace() == null) {
                    context.buildConstraintViolationWithTemplate("This code is running in Kubernetes or as a Kubernetes client "
                                                                     + "but it is not possible to resolve k8s namespace for backups!").addConstraintViolation();

                    return false;
                }
            }

            if (value.entities == null) {
                value.entities = DatabaseEntities.empty();
            }

            if (value.proxySettings != null) {

            }

            return true;
        }
    }
}
