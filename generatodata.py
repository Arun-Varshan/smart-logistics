
import pandas as pd
from prophet import Prophet
import matplotlib.pyplot as plt
import joblib

# Step 1: Load your CSV
# Replace with your actual file path
csv_path = r"C:\Users\Omen\OneDrive\Documents\Desktop\smart\data\daily_parcels.csv"  # ← change if needed
df = pd.read_csv(csv_path)

# Step 2: Clean and prepare columns
df.columns = df.columns.str.strip().str.lower()  # fix any spaces/case

# Convert date to datetime (your format is YYYY-MM-DD)
df['ds'] = pd.to_datetime(df['ds'], errors='coerce')

# Drop any invalid dates
df = df.dropna(subset=['ds'])

# Step 3: Aggregate to daily total volume
# Option A: If 'volume' is actual count/weight → sum it
daily_df = df.groupby('ds')['volume'].sum().reset_index(name='y')

# Option B: If volume is always 1 → just count number of parcels per day
# daily_df = df.groupby('ds').size().reset_index(name='y')

print("Daily aggregated data (first 5 rows):")
print(daily_df.head())

print(f"\nTotal days in data: {len(daily_df)}")

# Step 4: Train Prophet model
model = Prophet(
    yearly_seasonality=True,   # yearly patterns (festivals, etc.)
    weekly_seasonality=True,   # weekly cycles
    daily_seasonality=True,    # daily patterns
    seasonality_mode='additive',
    changepoint_prior_scale=0.05
)

# Add Indian holidays (very useful for e-commerce peaks)
model.add_country_holidays(country_name='IN')

# Fit the model
model.fit(daily_df)

# Step 5: Predict future (e.g., next 30 days including tomorrow)
future = model.make_future_dataframe(periods=30)
forecast = model.predict(future)

# Step 6: Show results
# Main forecast plot
fig1 = model.plot(forecast)
plt.title("Parcel Volume Forecast - Next 30 Days")
plt.xlabel("Date")
plt.ylabel("Predicted Parcels")
plt.grid(True)
plt.show()

# Components plot (trend + weekly + yearly)
fig2 = model.plot_components(forecast)
plt.show()

# Tomorrow's prediction
tomorrow = forecast.iloc[len(daily_df)]  # first future row
print("\nTomorrow's Prediction:")
print(f"Date: {tomorrow['ds'].date()}")
print(f"Predicted parcels: {round(tomorrow['yhat'])}")
print(f"Range: {round(tomorrow['yhat_lower'])} – {round(tomorrow['yhat_upper'])}")

# Step 7: Save the trained model
joblib.dump(model, 'prophet_parcel_model.pkl')
print("\nModel saved as: prophet_parcel_model.pkl")