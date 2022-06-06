/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.sampler.random;

import org.apache.commons.math3.exception.OutOfRangeException;
import org.apache.commons.math3.special.Beta;
import org.apache.commons.math3.special.Gamma;
import org.apache.commons.math3.util.FastMath;
import org.apache.commons.math3.util.MathUtils;


public class LongBinomialDistribution {
    /** 1/2 * log(2π). */
    private static final double HALF_LOG_2_PI = 0.5 * FastMath.log(MathUtils.TWO_PI);

    /** exact Stirling expansion error for certain values. */
    private static final double[] EXACT_STIRLING_ERRORS = {
        0.0, /* 0.0 */
        0.1534264097200273452913848, /* 0.5 */
        0.0810614667953272582196702, /* 1.0 */
        0.0548141210519176538961390, /* 1.5 */
        0.0413406959554092940938221, /* 2.0 */
        0.03316287351993628748511048, /* 2.5 */
        0.02767792568499833914878929, /* 3.0 */
        0.02374616365629749597132920, /* 3.5 */
        0.02079067210376509311152277, /* 4.0 */
        0.01848845053267318523077934, /* 4.5 */
        0.01664469118982119216319487, /* 5.0 */
        0.01513497322191737887351255, /* 5.5 */
        0.01387612882307074799874573, /* 6.0 */
        0.01281046524292022692424986, /* 6.5 */
        0.01189670994589177009505572, /* 7.0 */
        0.01110455975820691732662991, /* 7.5 */
        0.010411265261972096497478567, /* 8.0 */
        0.009799416126158803298389475, /* 8.5 */
        0.009255462182712732917728637, /* 9.0 */
        0.008768700134139385462952823, /* 9.5 */
        0.008330563433362871256469318, /* 10.0 */
        0.007934114564314020547248100, /* 10.5 */
        0.007573675487951840794972024, /* 11.0 */
        0.007244554301320383179543912, /* 11.5 */
        0.006942840107209529865664152, /* 12.0 */
        0.006665247032707682442354394, /* 12.5 */
        0.006408994188004207068439631, /* 13.0 */
        0.006171712263039457647532867, /* 13.5 */
        0.005951370112758847735624416, /* 14.0 */
        0.005746216513010115682023589, /* 14.5 */
        0.005554733551962801371038690 /* 15.0 */
    };

    private final long numberOfTrials;
    private final double probabilityOfSuccess;

    public LongBinomialDistribution(long numberOfTrials, double probabilityOfSuccess) {
        this.numberOfTrials = numberOfTrials;
        this.probabilityOfSuccess = probabilityOfSuccess;
    }

    /**
     * For a random variable X whose values are distributed according to this distribution,
     * this method returns log(P(X = x)), where log is the natural logarithm.
     * In other words, this method represents the logarithm of the probability mass function (PMF) for the distribution.
     * Note that due to the floating point precision and under/overflow issues,
     * this method will for some distributions be more precise and faster than computing the logarithm of probability(int).
     */
    public double logProbability(long x) {
        if (numberOfTrials == 0) {
            return (x == 0) ? 0. : Double.NEGATIVE_INFINITY;
        }
        double ret;
        if (x < 0 || x > numberOfTrials) {
            ret = Double.NEGATIVE_INFINITY;
        } else {
            ret = logBinomialProbability(x, numberOfTrials, probabilityOfSuccess, 1.0 - probabilityOfSuccess);
        }
        return ret;
    }

    public long inverseCumulativeProbability(final double p) throws OutOfRangeException {
        if (p < 0.0 || p > 1.0) {
            throw new OutOfRangeException(p, 0, 1);
        }

        long lower = -1;
        long upper = numberOfTrials;

        // use the one-sided Chebyshev inequality to narrow the bracket
        // cf. AbstractRealDistribution.inverseCumulativeProbability(double)
        final double mu = numberOfTrials * probabilityOfSuccess;
        final double sigma = FastMath.sqrt(numberOfTrials * probabilityOfSuccess * (1 - probabilityOfSuccess));
        double k = FastMath.sqrt((1.0 - p) / p);
        double tmp = mu - k * sigma;
        if (tmp > lower) {
            lower = ((int) FastMath.ceil(tmp)) - 1;
        }
        k = 1.0 / k;
        tmp = mu + k * sigma;
        if (tmp < upper) {
            upper = ((int) FastMath.ceil(tmp)) - 1;
        }
        return solveInverseCumulativeProbability(p, lower, upper);
    }

    public double cumulativeProbability(long x) {
        double ret;
        if (x < 0) {
            ret = 0.0;
        } else if (x >= numberOfTrials) {
            ret = 1.0;
        } else {
            ret = 1.0 - Beta.regularizedBeta(probabilityOfSuccess, x + 1.0, numberOfTrials - x);
        }
        return ret;
    }

    protected long solveInverseCumulativeProbability(final double p, long lower, long upper) {
        while (lower + 1 < upper) {
            long xm = (lower + upper) / 2;
            if (xm < lower || xm > upper) {
                /*
                 * Overflow.
                 * There will never be an overflow in both calculation methods
                 * for xm at the same time
                 */
                xm = lower + (upper - lower) / 2;
            }

            double pm = cumulativeProbability(xm);
            if (Double.isNaN(pm)) {
                throw new IllegalStateException("cdf of binomial distrib threw NaN");
            }
            if (pm >= p) {
                upper = xm;
            } else {
                lower = xm;
            }
        }
        return upper;
    }

    /**
     * A part of the deviance portion of the saddle point approximation.
     * References:
     * Catherine Loader (2000). "Fast and Accurate Computation of Binomial Probabilities.". http://www.herine.net/stat/papers/dbinom.pdf
     * @param x – the x value.
     * @param mu – the average.
     * @return : a part of the deviance.
     */
    static double getDeviancePart(double x, double mu) {
        double ret;
        if (FastMath.abs(x - mu) < 0.1 * (x + mu)) {
            double d = x - mu;
            double v = d / (x + mu);
            double s1 = v * d;
            double s = Double.NaN;
            double ej = 2.0 * x * v;
            v *= v;
            int j = 1;
            while (s1 != s) {
                s = s1;
                ej *= v;
                s1 = s + ej / ((j * 2) + 1);
                ++j;
            }
            ret = s1;
        } else {
            ret = x * FastMath.log(x / mu) + mu - x;
        }
        return ret;
    }

    /**
     * Compute the error of Stirling's series at the given value.
     * References:
     * Eric W. Weisstein. "Stirling's Series." From MathWorld--A Wolfram Web
     * Resource. http://mathworld.wolfram.com/StirlingsSeries.html
     *
     * @param z the value.
     * @return the Striling's series error.
     */
    static double getStirlingError(double z) {
        double ret;
        if (z < 15.0) {
            double z2 = 2.0 * z;
            if (FastMath.floor(z2) == z2) {
                ret = EXACT_STIRLING_ERRORS[(int) z2];
            } else {
                ret = Gamma.logGamma(z + 1.0) - (z + 0.5) * FastMath.log(z) + z - HALF_LOG_2_PI;
            }
        } else {
            double z2 = z * z;
            ret = (0.083333333333333333333 - (0.00277777777777777777778 - (0.00079365079365079365079365 - (0.000595238095238095238095238
                - 0.0008417508417508417508417508 / z2) / z2) / z2) / z2) / z;
        }
        return ret;
    }

    /**
     * Compute the logarithm of the PMF for a binomial distribution using the saddle point expansion.
     * Params:
     * @param x – the value at which the probability is evaluated.
     * @param n – the number of trials.
     * @param p – the probability of success.
     * @param q – the probability of failure (1 - p).
     * @return : log(p(x)).
     */
    static double logBinomialProbability(long x, long n, double p, double q) {
        double ret;
        if (x == 0) {
            if (p < 0.1) {
                ret = -getDeviancePart(n, n * q) - n * p;
            } else {
                ret = n * FastMath.log(q);
            }
        } else if (x == n) {
            if (q < 0.1) {
                ret = -getDeviancePart(n, n * p) - n * q;
            } else {
                ret = n * FastMath.log(p);
            }
        } else {
            ret = getStirlingError(n) - getStirlingError(x) - getStirlingError(n - x) - getDeviancePart(x, n * p) - getDeviancePart(
                n - x,
                n * q
            );
            double f = (MathUtils.TWO_PI * x * (n - x)) / n;
            ret = -0.5 * FastMath.log(f) + ret;
        }
        return ret;
    }
}
