package com.instaclustr.esop.impl.restore;

import static com.instaclustr.esop.impl.restore.RestorationStrategy.RestorationStrategyType.HARDLINKS;
import static com.instaclustr.esop.impl.restore.RestorationStrategy.RestorationStrategyType.IMPORT;
import static com.instaclustr.esop.impl.restore.RestorationStrategy.RestorationStrategyType.IN_PLACE;
import static com.instaclustr.esop.impl.restore.RestorationStrategy.RestorationStrategyType.UNKNOWN;
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

import com.instaclustr.esop.impl.DatabaseEntities;
import com.instaclustr.esop.impl.RenamedEntities;
import com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseType;
import com.instaclustr.kubernetes.KubernetesHelper;

@Target({TYPE, PARAMETER})
@Retention(RUNTIME)
@Constraint(validatedBy = {
    ValidRestoreOperationRequest.RestoreOperationRequestValidator.class,
})
public @interface ValidRestoreOperationRequest {

    String message() default "{com.instaclustr.esop.impl.restore.ValidRestoreOperationRequest.RestoreOperationRequestValidator.message}";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

    final class RestoreOperationRequestValidator implements ConstraintValidator<ValidRestoreOperationRequest, RestoreOperationRequest> {

        @Override
        public boolean isValid(final RestoreOperationRequest value, final ConstraintValidatorContext context) {

            context.disableDefaultConstraintViolation();

            if (value.restorationPhase == null) {
                value.restorationPhase = RestorationPhaseType.UNKNOWN;
            }

            if (value.restorationStrategyType == UNKNOWN) {
                context.buildConstraintViolationWithTemplate("restorationStrategyType is not recognized").addConstraintViolation();
                return false;
            }

            if (value.restorationStrategyType != IN_PLACE) {
                if (value.restorationPhase == RestorationPhaseType.UNKNOWN) {
                    context.buildConstraintViolationWithTemplate("restorationPhase is not recognized, it has to be set when you use IMPORT or HARDLINKS strategy type").addConstraintViolation();
                    return false;
                }
            }

            if (value.restorationStrategyType == IN_PLACE && value.restorationPhase != RestorationPhaseType.UNKNOWN) {
                context.buildConstraintViolationWithTemplate(format("you can not set restorationPhase %s when your restorationStrategyType is IN_PLACE",
                                                                    value.restorationPhase)).addConstraintViolation();
                return false;
            }

            if (value.restorationStrategyType == IMPORT || value.restorationStrategyType == HARDLINKS) {
                if (value.importing == null) {
                    context.buildConstraintViolationWithTemplate(format("you can not specify %s restorationStrategyType and have 'import' field empty!",
                                                                        value.restorationStrategyType));
                    return false;
                }
            }

            if (!Files.exists(value.cassandraDirectory)) {
                context.buildConstraintViolationWithTemplate(format("cassandraDirectory %s does not exist", value.cassandraDirectory)).addConstraintViolation();
                return false;
            }

            if ((KubernetesHelper.isRunningInKubernetes() || KubernetesHelper.isRunningAsClient())) {

                if (value.resolveKubernetesSecretName() == null) {
                    context.buildConstraintViolationWithTemplate("This code is running in Kubernetes or as a Kubernetes client "
                                                                     + "but it is not possible to resolve k8s secret name for restores!").addConstraintViolation();

                    return false;
                }

                if (value.resolveKubernetesNamespace() == null) {
                    context.buildConstraintViolationWithTemplate("This code is running in Kubernetes or as a Kubernetes client "
                                                                     + "but it is not possible to resolve k8s namespace for restores!").addConstraintViolation();

                    return false;
                }
            }

            if (value.entities == null) {
                value.entities = DatabaseEntities.empty();
            }

            try {
                DatabaseEntities.validateForRequest(value.entities);
            } catch (final Exception ex) {
                context.buildConstraintViolationWithTemplate(ex.getMessage()).addConstraintViolation();
                return false;
            }

            try {
                RenamedEntities.validate(value.rename);
            } catch (final Exception ex) {
                context.buildConstraintViolationWithTemplate("Invalid 'rename' parameter: " + ex.getMessage()).addConstraintViolation();
                return false;
            }

            if (value.rename != null && !value.rename.isEmpty() && value.restorationStrategyType == IN_PLACE) {
                context.buildConstraintViolationWithTemplate("rename field can not be used for in-place strategy, only for import or hardlinks").addConstraintViolation();
                return false;
            }

            return true;
        }
    }
}
