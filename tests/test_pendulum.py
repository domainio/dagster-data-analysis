import pendulum
import dagster

print(f"Pendulum version: {pendulum.__version__}")
print(f"Dagster version: {dagster.__version__}")
print(dir(pendulum))

# Try to use Pendulum
now = pendulum.now()
print(f"Current time: {now}")

# Try to use Dagster's Pendulum wrapper
from dagster._seven import PendulumDateTime
print(f"PendulumDateTime: {PendulumDateTime}")