import numpy as np

# Example PnLs per trade or per day
pnl_list = [100, -50, 80, -30, 60, 120, -70]

# Step 1: Create cumulative equity curve (starting from 0 or your initial capital)
equity_curve = np.cumsum(pnl_list)

print(equity_curve)

# Step 2: Track running peak of equity
running_max = np.maximum.accumulate(equity_curve)

# Step 3: Calculate drawdowns
drawdowns = (equity_curve - running_max) / running_max

# Step 4: Maximum drawdown (most negative drawdown)
max_drawdown = np.min(drawdowns)

# Optional: Show as percentage
print(f"Equity Curve: {equity_curve}")
print(f"Drawdowns: {drawdowns}")
print(f"Max Drawdown: {max_drawdown:.2%}")
