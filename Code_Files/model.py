"""
Delta Live Tables Dimensional & Fact Modeling.
Implements Star Schema with SCD Type 1 and Type 2 CDC flows for analytical reporting.
"""

from pyspark import pipelines as dp


# ---------------------------------------------------------
# Dimension: Passenger (SCD Type 1)
# ---------------------------------------------------------
@dp.view
def dim_passenger_view():
    """Extracts unique passenger records from silver OBT."""
    df = spark.readStream.table("silver_obt")
    return df.select(
        "passenger_id", "passenger_name", "passenger_email", "passenger_phone"
    ).dropDuplicates(subset=["passenger_id"])


dp.create_streaming_table("dim_passenger")
dp.create_auto_cdc_flow(
    target="dim_passenger",
    source="dim_passenger_view",
    keys=["passenger_id"],
    sequence_by="passenger_id",
    stored_as_scd_type=1,
)


# ---------------------------------------------------------
# Dimension: Driver (SCD Type 1)
# ---------------------------------------------------------
@dp.view
def dim_driver_view():
    """Extracts unique driver records from silver OBT."""
    df = spark.readStream.table("uber.bronze.silver_obt")
    return df.select(
        "driver_id", "driver_name", "driver_rating", "driver_phone", "driver_license"
    ).dropDuplicates(subset=["driver_id"])


dp.create_streaming_table("dim_driver")
dp.create_auto_cdc_flow(
    target="dim_driver",
    source="dim_driver_view",
    keys=["driver_id"],
    sequence_by="driver_id",
    stored_as_scd_type=1,
)


# ---------------------------------------------------------
# Dimension: Vehicle (SCD Type 1)
# ---------------------------------------------------------
@dp.view
def dim_vehicle_view():
    """Extracts unique vehicle records from silver OBT."""
    df = spark.readStream.table("uber.bronze.silver_obt")
    return df.select(
        "vehicle_id",
        "vehicle_make_id",
        "vehicle_type_id",
        "vehicle_model",
        "vehicle_color",
        "license_plate",
        "vehicle_make",
        "vehicle_type",
    ).dropDuplicates(subset=["vehicle_id"])


dp.create_streaming_table("dim_vehicle")
dp.create_auto_cdc_flow(
    target="dim_vehicle",
    source="dim_vehicle_view",
    keys=["vehicle_id"],
    sequence_by="vehicle_id",
    stored_as_scd_type=1,
)


# ---------------------------------------------------------
# Dimension: Payment Method (SCD Type 1)
# ---------------------------------------------------------
@dp.view
def dim_payment_view():
    """Extracts unique payment method records from silver OBT."""
    df = spark.readStream.table("uber.bronze.silver_obt")
    return df.select(
        "payment_method_id", "payment_method", "is_card", "requires_auth"
    ).dropDuplicates(subset=["payment_method_id"])


dp.create_streaming_table("dim_payment")
dp.create_auto_cdc_flow(
    target="dim_payment",
    source="dim_payment_view",
    keys=["payment_method_id"],
    sequence_by="payment_method_id",
    stored_as_scd_type=1,
)


# ---------------------------------------------------------
# Dimension: Booking (SCD Type 1)
# ---------------------------------------------------------
@dp.view
def dim_booking_view():
    """Extracts unique ride booking records from silver OBT."""
    df = spark.readStream.table("uber.bronze.silver_obt")
    return df.select(
        "ride_id",
        "confirmation_number",
        "dropoff_location_id",
        "ride_status_id",
        "dropoff_city_id",
        "cancellation_reason_id",
        "dropoff_address",
        "dropoff_latitude",
        "dropoff_longitude",
        "booking_timestamp",
        "dropoff_timestamp",
        "pickup_address",
        "pickup_latitude",
        "pickup_longitude",
        "pickup_location_id",
    ).dropDuplicates(subset=["ride_id"])


dp.create_streaming_table("dim_booking")
dp.create_auto_cdc_flow(
    target="dim_booking",
    source="dim_booking_view",
    keys=["ride_id"],
    sequence_by="ride_id",
    stored_as_scd_type=1,
)


# ---------------------------------------------------------
# Dimension: Location (SCD Type 2 with historical tracking)
# ---------------------------------------------------------
@dp.table
def dim_location_view():
    """Extracts location records with temporal updates."""
    df = spark.readStream.table("uber.bronze.silver_obt")
    return df.select(
        "pickup_city_id", "pickup_city", "city_updated_at", "region", "state"
    ).dropDuplicates(subset=["pickup_city_id", "city_updated_at"])


dp.create_streaming_table("dim_location")
dp.create_auto_cdc_flow(
    target="dim_location",
    source="dim_location_view",
    keys=["pickup_city_id"],
    sequence_by="city_updated_at",
    stored_as_scd_type=2,
)


# ---------------------------------------------------------
# Fact: Rides
# ---------------------------------------------------------
@dp.view
def fact_view():
    """Extracts fact measures and surrogate keys from silver OBT."""
    df = spark.readStream.table("uber.bronze.silver_obt")
    return df.select(
        "ride_id",
        "pickup_city_id",
        "payment_method_id",
        "driver_id",
        "passenger_id",
        "vehicle_id",
        "distance_miles",
        "duration_minutes",
        "base_fare",
        "distance_fare",
        "time_fare",
        "surge_multiplier",
        "total_fare",
        "tip_amount",
        "rating",
        "base_rate",
        "per_mile",
        "per_minute",
    )


dp.create_streaming_table("fact")
dp.create_auto_cdc_flow(
    target="fact",
    source="fact_view",
    keys=[
        "ride_id",
        "pickup_city_id",
        "payment_method_id",
        "driver_id",
        "passenger_id",
        "vehicle_id",
    ],
    sequence_by="ride_id",
    stored_as_scd_type=1,
)

