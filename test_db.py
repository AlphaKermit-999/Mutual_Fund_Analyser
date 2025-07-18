from core.database import get_all_fund_data, get_nav_history_by_code
import time

print("--- Testing the database interface ---")

# Test 1: Fetch all fund data
print("\n[Test 1] Fetching all fund names...")
start_time = time.time()
all_funds = get_all_fund_data()
end_time = time.time()
if not all_funds.empty:
    print(f"✅ Success! Found {len(all_funds)} funds in {end_time - start_time:.2f} seconds.")
    print("Sample data:")
    print(all_funds.head())
else:
    print("❌ Failure! Could not fetch fund data.")

# Test 2: Fetch a valid fund's NAV history
# Let's use a known scheme code, e.g., Axis Bluechip Fund
VALID_SCHEME_CODE = 120503 
print(f"\n[Test 2] Fetching NAV history for a VALID scheme code: {VALID_SCHEME_CODE}")
start_time = time.time()
nav_history = get_nav_history_by_code(VALID_SCHEME_CODE)
end_time = time.time()
if not nav_history.empty:
    print(f"✅ Success! Found {len(nav_history)} NAV records in {end_time - start_time:.2f} seconds.")
    print("Most recent NAV data:")
    print(nav_history.tail())
else:
    print(f"❌ Failure! Could not fetch NAV history for scheme code {VALID_SCHEME_CODE}.")

# Test 3: Fetch an invalid fund's NAV history
INVALID_SCHEME_CODE = -1
print(f"\n[Test 3] Fetching NAV history for an INVALID scheme code: {INVALID_SCHEME_CODE}")
invalid_history = get_nav_history_by_code(INVALID_SCHEME_CODE)
if invalid_history.empty:
    print("✅ Success! The function correctly returned an empty DataFrame for an invalid code.")
else:
    print("❌ Failure! The function returned data for an invalid code.")

print("\n--- Database interface tests complete ---")