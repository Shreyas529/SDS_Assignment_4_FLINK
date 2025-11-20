import numpy as np

class MMPPGenerator:
    def __init__(self, avg_rate, rate_ratio=3.0, q12=1.0, q21=1.0, window=1.0, seed=None):
        """
        Stateful MMPP sampler that behaves like np.random.poisson.
        
        Args:
            avg_rate (float): Desired average throughput (mean count per window).
            rate_ratio (float): λ_high / λ_low.
            q12 (float): Transition rate from state 1 -> 2.
            q21 (float): Transition rate from state 2 -> 1.
            window (float): Sampling interval length.
            seed (int): RNG seed.
        """
        self.rng = np.random.default_rng(seed)
        self.window = window

        # Stationary distribution of CTMC
        pi1 = q21 / (q12 + q21)
        pi2 = q12 / (q12 + q21)

        # Solve for λ1, λ2 such that average = avg_rate
        lam1 = avg_rate / (pi1 + pi2 * rate_ratio)
        lam2 = rate_ratio * lam1
        self.lambdas = [lam1, lam2]

        # CTMC generator matrix
        self.Q = np.array([[-q12, q12],
                           [q21, -q21]])

        # Start in state 0
        self.state = 0

    def sample(self):
        """Return one correlated MMPP sample (like np.random.poisson)."""
        lam = self.lambdas[self.state]

        # Poisson arrivals in this window
        count = self.rng.poisson(lam * self.window)

        # Update CTMC state (possible transition during this window)
        rate_out = -self.Q[self.state, self.state]
        if self.rng.random() < 1 - np.exp(-rate_out * self.window):
            probs = self.Q[self.state, :] / rate_out
            probs[self.state] = 0
            self.state = self.rng.choice([0, 1], p=probs)

        return count


# Example usage:
if __name__ == "__main__":
    mmpp = MMPPGenerator(avg_rate=100)
    samples = [mmpp.sample() for _ in range(1000)]
    print(samples)
    print("Empirical mean:", np.mean(samples))
