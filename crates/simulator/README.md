🔁 1. Stateless / Memoryless Models
These generate values independently at each time step.

Model	Description	Use Case
Uniform Random	Random values in [min, max]	Noise, binary states
Gaussian Noise	Values from normal distribution around a mean	Sensor noise, measurement error
Poisson Process	Time between events follows exponential distribution	Counting events (e.g. traffic, clicks)
Exponential Decay	Simulates decay of a value over time (e.g. voltage drop)	Battery, radiation decay

🔁 2. Periodic / Seasonal Models
Model	Description	Use Case
Sine Wave (Seasonal)	Sinusoidal oscillation with adjustable frequency, amplitude, and phase	Temperature, energy use, tides
Sawtooth Wave	Linearly rising and falling pattern	Cyclical production, PWM
Square Wave	Alternating high/low states	Binary actuators, logic signals
Day/Night Cycle	Daily pattern with Gaussian or sine-based daylight simulation	Outdoor light, solar energy

🔀 3. Stateful / Time-Dependent Models
Model	Description	Use Case
Random Walk	Value at t+1 = t + noise, simulates drifting values	Temperature, stock prices
Brownian Motion	Random walk with continuous distribution	Finance, particle simulation
ARIMA	Auto-Regressive Integrated Moving Average model	Time series with autocorrelation
Markov Chain	Transition between discrete states	State machines, fault simulation
Hidden Markov Models	Probabilistic transitions with hidden state	Activity recognition, failure modes
Logistic Growth	Sigmoid-shaped curve with saturation	Population, charging curves

🧠 4. Domain-Specific / Hybrid Models
Model	Description	Use Case
PID Response	Control system simulation (P, I, D terms)	Thermostats, actuators
Kalman Filter	Recursive estimate from noisy measurements	Sensor fusion, estimation
Anomaly Injection	Injects rare abnormal spikes/dips	Fault injection, testing resilience
Behavior Cloning	Replay real data with added noise	Simulating learned behaviors

💡 5. Composite & Custom Models
You can combine models for complex simulations:

🌡️ seasonal_sine + noise + random_drift

🔋 exponential_decay + random_spikes

🚗 logistic_growth + daily_cycle + failure_event

📈 trend + gaussian_noise + anomaly_spike

These allow realistic, tunable, and probabilistic simulations of nearly any physical sensor.


🧰 Tools You Can Use in Rust
rand: Random number generators (uniform, normal, Poisson, etc.)

statrs: Statistics + probability distributions

ndarray: Multivariate data handling

nalgebra: Linear algebra for matrix-based simulations (e.g. Kalman filters)

chrono: Time manipulation

serde_json: Configuring models via JSON
