from typing import Callable, Dict, List, Type

from hyperscale.versioning.flags.types.unstable.flag import unstable

from .distribution_types import DistributionMap, DistributionTypes
from .distributions.types import (
    AlphaDistribution,
    AnglitDistribution,
    ArcsineDistribution,
    ArgusDistribution,
    BetaDistribution,
    BetaPrimeDistribution,
    BradfordDistribution,
    Burr12Distribution,
    BurrDistribution,
    CauchyDistribution,
    ChiDistribution,
    ChiSquaredDistribution,
    CosineDistribution,
    CrystalDistribution,
    DGammaDistribution,
    DWeibullDistribution,
    ErlangDistribution,
    ExponentialDistribution,
    ExponentialNormalDistribution,
    ExponentialPowerDistribution,
    FatigueLifeDistribution,
    FDistribution,
    FiskDistribution,
    FoldedCauchyDistribution,
    FoldedNormalDistribution,
    GammaDistribution,
    GaussHypergeometricDistribution,
    GeneralizedExponentialDistribution,
    GeneralizedExtremeDistribution,
    GeneralizedGammaDistribution,
    GeneralizedHalfLogisticDistribution,
    GeneralizedHyperbolicDistribution,
    GeneralizedInverseGaussDistribution,
    GeneralizedLogisticDistribution,
    GeneralizedNormalDistribution,
    GeneralizedParetoDistribution,
    GibratDistribution,
    GompertzDistribution,
    GumbelLDistribution,
    GumbelRDistribution,
    HalfCauchyDistribution,
    HalfGeneralizedNormalDistribution,
    HalfLogisticDistribution,
    HalfNormalDistribution,
    HyperbolicSecantDistribution,
    InverseGammaDistribution,
    InverseWeibullDistribution,
    JohnsonSBDistribution,
    JohnsonSUDistribution,
    Kappa3Distribution,
    Kappa4Distribution,
    KSOneDistribution,
    KSTwoBinomialGeneralizedDistribution,
    KSTwoDistribution,
    LaplaceAsymmetricDistribution,
    LaplaceDistribution,
    LevyDistribution,
    LevyLDistribution,
    LevyStableDistribution,
    LogGammaDistribution,
    LogisticDistribution,
    LogLaplaceDistribution,
    LogUniformDistribution,
    LomaxDistribution,
    MaxwellDistribution,
    MielkeDistribution,
    MoyalDistribution,
    NakagamiDistribution,
    NonCenteralChiSquaredDistribution,
    NonCenteralFDistribution,
    NonCenteralTDistribution,
    NormalDistribution,
    NormalInverseGaussDistribution,
    ParetoDistribution,
    Pearson3Distribution,
    PowerlawDistribution,
    PowerLogNormalDistribution,
    PowerNormalDistribution,
    RayleighDistribution,
    RDistribution,
    ReciprocalInverseGaussDistribution,
    RiceDistribution,
    SemiCircularDistribution,
    SkewedCauchyDistribution,
    SkewedNormalDistribution,
    StudentRangeDistribution,
    TDistribution,
    TrapezoidDistribution,
    TriangularDistribution,
    TruncatedExponentialDistribution,
    TruncatedNormalDistribution,
    TruncatedParetoDistribution,
    TruncatedWeibullMinimumDistribution,
    TukeyLambdaDistribution,
    UniformDistribution,
    VonMisesDistribution,
    VonMisesLineDistribution,
    WaldDistribution,
    WeibullMaxmimumDistribution,
    WeibullMinimumDistribution,
    WrappedCauchyDistribution,
)
from .distributions.types.base import BaseDistribution


@unstable
class Distribution:
    def __init__(
        self,
        distribution_type: DistributionTypes = DistributionTypes.NORMAL,
        intervals: int = None,
    ) -> None:
        self._distribution_map = DistributionMap()
        self._distributions: Dict[str, Callable[..., List[float]]] = {
            DistributionTypes.ALPHA: lambda size: AlphaDistribution(size),
            DistributionTypes.ANGLIT: lambda size: AnglitDistribution(size),
            DistributionTypes.ARCSINE: lambda size: ArcsineDistribution(size),
            DistributionTypes.ARGUS: lambda size: ArgusDistribution(size),
            DistributionTypes.BETA_PRIME: lambda size: BetaPrimeDistribution(size),
            DistributionTypes.BETA: lambda size: BetaDistribution(size),
            DistributionTypes.BRADFORD: lambda size: BradfordDistribution(size),
            DistributionTypes.BURR: lambda size: BurrDistribution(size),
            DistributionTypes.BURR12: lambda size: Burr12Distribution(size),
            DistributionTypes.CAUCHY: lambda size: CauchyDistribution(size),
            DistributionTypes.CHI_SQUARED: lambda size: ChiSquaredDistribution(size),
            DistributionTypes.CHI: lambda size: ChiDistribution(size),
            DistributionTypes.COSINE: lambda size: CosineDistribution(size),
            DistributionTypes.CRYSTAL_BALL: lambda size: CrystalDistribution(size),
            DistributionTypes.DGAMMA: lambda size: DGammaDistribution(size),
            DistributionTypes.DWEIBULL: lambda size: DWeibullDistribution(size),
            DistributionTypes.ERLANG: lambda size: ErlangDistribution(size),
            DistributionTypes.EXPONENTIAL_NORMAL: lambda size: ExponentialNormalDistribution(
                size
            ),
            DistributionTypes.EXPONENTIAL_POWER: lambda size: ExponentialPowerDistribution(
                size
            ),
            DistributionTypes.EXPONENTIAL: lambda size: ExponentialDistribution(size),
            DistributionTypes.F_DISTRIBUTION: lambda size: FDistribution(size),
            DistributionTypes.FATIGUE_LIFE: lambda size: FatigueLifeDistribution(size),
            DistributionTypes.FISK: lambda size: FiskDistribution(size),
            DistributionTypes.FOLDED_CAUCHY: lambda size: FoldedCauchyDistribution(
                size
            ),
            DistributionTypes.FOLDED_NORMAL: lambda size: FoldedNormalDistribution(
                size
            ),
            DistributionTypes.GAMMA: lambda size: GammaDistribution(size),
            DistributionTypes.GAUSS_HYPERGEOMETRIC: lambda size: GaussHypergeometricDistribution(
                size
            ),
            DistributionTypes.GENERALIZED_EXPONENTIAL: lambda size: GeneralizedExponentialDistribution(
                size
            ),
            DistributionTypes.GENERALIZED_EXTREME: lambda size: GeneralizedExtremeDistribution(
                size
            ),
            DistributionTypes.GENERALIZED_GAMMA: lambda size: GeneralizedGammaDistribution(
                size
            ),
            DistributionTypes.GENERALIZED_HALF_LOGISTIC: lambda size: GeneralizedHalfLogisticDistribution(
                size
            ),
            DistributionTypes.GENERALIZED_HYPERBOLIC: lambda size: GeneralizedHyperbolicDistribution(
                size
            ),
            DistributionTypes.GENERALIZED_INVERSE_GAUSS: lambda size: GeneralizedInverseGaussDistribution(
                size
            ),
            DistributionTypes.GENERALIZED_LOGISTIC: lambda size: GeneralizedLogisticDistribution(
                size
            ),
            DistributionTypes.GENERALIZED_NORMAL: lambda size: GeneralizedNormalDistribution(
                size
            ),
            DistributionTypes.GENERALIZED_PARETO: lambda size: GeneralizedParetoDistribution(
                size
            ),
            DistributionTypes.GIBRAT: lambda size: GibratDistribution(size),
            DistributionTypes.GOMPERTZ: lambda size: GompertzDistribution(size),
            DistributionTypes.GUMBEL_L: lambda size: GumbelLDistribution(size),
            DistributionTypes.GUMBEL_R: lambda size: GumbelRDistribution(size),
            DistributionTypes.HALF_CAUCHY: lambda size: HalfCauchyDistribution(size),
            DistributionTypes.HALF_GENERALIZED_NORMAL: lambda size: HalfGeneralizedNormalDistribution(
                size
            ),
            DistributionTypes.HALF_LOGISTIC: lambda size: HalfLogisticDistribution(
                size
            ),
            DistributionTypes.HALF_NORMAL: lambda size: HalfNormalDistribution(size),
            DistributionTypes.HYPERBOLIC_SECANT: lambda size: HyperbolicSecantDistribution(
                size
            ),
            DistributionTypes.INVERSE_GAMMA: lambda size: InverseGammaDistribution(
                size
            ),
            DistributionTypes.INVERSE_WEIBULL: lambda size: InverseWeibullDistribution(
                size
            ),
            DistributionTypes.JOHNSON_SB: lambda size: JohnsonSBDistribution(size),
            DistributionTypes.JOHNSON_SU: lambda size: JohnsonSUDistribution(size),
            DistributionTypes.KAPPA_3: lambda size: Kappa3Distribution(size),
            DistributionTypes.KAPPA_4: lambda size: Kappa4Distribution(size),
            DistributionTypes.KS_ONE: lambda size: KSOneDistribution(size),
            DistributionTypes.KS_TWO: lambda size: KSTwoDistribution(size),
            DistributionTypes.KS_TWO_BIGENERALIZED: lambda size: KSTwoBinomialGeneralizedDistribution(
                size
            ),
            DistributionTypes.LAPLACE: lambda size: LaplaceDistribution(size),
            DistributionTypes.LAPLACE_ASYMMETRIC: lambda size: LaplaceAsymmetricDistribution(
                size
            ),
            DistributionTypes.LEVY: lambda size: LevyDistribution(size),
            DistributionTypes.LEVY_L: lambda size: LevyLDistribution(size),
            DistributionTypes.LEVY_STABLE: lambda size: LevyStableDistribution(size),
            DistributionTypes.LOG_GAMMA: lambda size: LogGammaDistribution(size),
            DistributionTypes.LOG_LAPLAPCE: lambda size: LogLaplaceDistribution(size),
            DistributionTypes.LOG_UNIFORM: lambda size: LogUniformDistribution(size),
            DistributionTypes.LOGISTIC: lambda size: LogisticDistribution(size),
            DistributionTypes.LOMAX: lambda size: LomaxDistribution(size),
            DistributionTypes.MAXWELL: lambda size: MaxwellDistribution(size),
            DistributionTypes.MIELKE: lambda size: MielkeDistribution(size),
            DistributionTypes.MOYAL: lambda size: MoyalDistribution(size),
            DistributionTypes.NAKAGAMI: lambda size: NakagamiDistribution(size),
            DistributionTypes.NON_CENTRAL_CHI_SQUARED: lambda size: NonCenteralChiSquaredDistribution(
                size
            ),
            DistributionTypes.NON_CENTRAL_F_DISTRIBUTION: lambda size: NonCenteralFDistribution(
                size
            ),
            DistributionTypes.NON_CENTRAL_T_DISTRIBUTION: lambda size: NonCenteralTDistribution(
                size
            ),
            DistributionTypes.NORMAL_INVERSE_GAUSS: lambda size: NormalInverseGaussDistribution(
                size
            ),
            DistributionTypes.NORMAL: lambda size: NormalDistribution(size),
            DistributionTypes.PARETO: lambda size: ParetoDistribution(size),
            DistributionTypes.PEARSON_3: lambda size: Pearson3Distribution(size),
            DistributionTypes.POWER_LOG_NORMAL: lambda size: PowerLogNormalDistribution(
                size
            ),
            DistributionTypes.POWER_NORMAL: lambda size: PowerNormalDistribution(size),
            DistributionTypes.POWERLAW: lambda size: PowerlawDistribution(size),
            DistributionTypes.R_DISTRIBUTION: lambda size: RDistribution(size),
            DistributionTypes.RAYLEIGH: lambda size: RayleighDistribution(size),
            DistributionTypes.RECIPROCAL_INVERSE_GAUSS: lambda size: ReciprocalInverseGaussDistribution(
                size
            ),
            DistributionTypes.RICE: lambda size: RiceDistribution(size),
            DistributionTypes.SEMI_CIRCULAR: lambda size: SemiCircularDistribution(
                size
            ),
            DistributionTypes.SKEWED_CAUCHY: lambda size: SkewedCauchyDistribution(
                size
            ),
            DistributionTypes.SKEWED_NORMAL: lambda size: SkewedNormalDistribution(
                size
            ),
            DistributionTypes.STUDENT_RANGE: lambda size: StudentRangeDistribution(
                size
            ),
            DistributionTypes.T_DISTRIBUTION: lambda size: TDistribution(size),
            DistributionTypes.TRAPEZOID: lambda size: TrapezoidDistribution(size),
            DistributionTypes.TRIANGULAR: lambda size: TriangularDistribution(size),
            DistributionTypes.TRUNCATED_EXPONENTIAL: lambda size: TruncatedExponentialDistribution(
                size
            ),
            DistributionTypes.TRUNCATED_NORMAL: lambda size: TruncatedNormalDistribution(
                size
            ),
            DistributionTypes.TRUNCATED_PARETO: lambda size: TruncatedParetoDistribution(
                size
            ),
            DistributionTypes.TRUNCATED_WEIBULL_MINIMUM: lambda size: TruncatedWeibullMinimumDistribution(
                size
            ),
            DistributionTypes.TUKEY_LAMBDA: lambda size: TukeyLambdaDistribution(size),
            DistributionTypes.UNIFORM: lambda size: UniformDistribution(size),
            DistributionTypes.VONMISES_LINE: lambda size: VonMisesLineDistribution(
                size
            ),
            DistributionTypes.VONMISES: lambda size: VonMisesDistribution(size),
            DistributionTypes.WALD: lambda size: WaldDistribution(size),
            DistributionTypes.WEIBULL_MAXIMUM: lambda size: WeibullMaxmimumDistribution(
                size
            ),
            DistributionTypes.WEIBULL_MINIMUM: lambda size: WeibullMinimumDistribution(
                size
            ),
            DistributionTypes.WRAPPED_CAUCHY: lambda size: WrappedCauchyDistribution(
                size
            ),
        }

        self.selected_distribution: DistributionTypes = self._distribution_map.get(
            distribution_type
        )
        self.disribution_function: Type[BaseDistribution] = self._distributions.get(
            self.selected_distribution
        )

        self.intervals = intervals

    def generate(self, batch_size: int):
        distribution = self.disribution_function(self.intervals)
        return distribution.generate_distribution(batch_size)
