import pandas as pd
import numpy as np
import logging
from config import RISK_FREE_RATE
from .database import get_nav_history_by_code

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_fund_scorecard(scheme_code: int) -> dict:
    """
    Orchestrates all calculations and generates the final scorecard for a given fund.
    This function includes a master try-except block to act as a final safety net.
    """
    try:
        nav_history = get_nav_history_by_code(scheme_code)
        
        # This initial check is a form of "early exit" error handling.
        if nav_history.empty or 'nav' not in nav_history.columns:
            return {"error": "NAV data is empty or invalid for this fund."}
            
        nav_series = nav_history['nav']
        
        # --- CALCULATIONS ---
        returns = calculate_returns_robust(nav_series)
        sharpe = calculate_sharpe_ratio(nav_series)
        volatility = nav_series.pct_change().std() * np.sqrt(252)

        # --- RULE-BASED SCORING MODEL ---
        score_components = {}
        
        perf_score = 0
        if returns.get('1 Year', 0) > 0.20: perf_score += 25
        elif returns.get('1 Year', 0) > 0.10: perf_score += 10
        if returns.get('3 Years', 0) > 0.15: perf_score += 25
        elif returns.get('3 Years', 0) > 0.10: perf_score += 15
        score_components['Performance'] = perf_score
        
        risk_score = 0
        if pd.notna(sharpe) and sharpe > 1.0: risk_score = 50
        elif pd.notna(sharpe) and sharpe > 0.75: risk_score = 35
        elif pd.notna(sharpe) and sharpe > 0.5: risk_score = 20
        score_components['Risk-Adjusted Return'] = risk_score
        
        final_score = sum(score_components.values())
        
        return {
            "metrics": {**returns, "sharpe_ratio": sharpe, "annual_volatility": volatility},
            "scores": {"components": score_components, "final_score": final_score}
        }
    except Exception as e:
        # This is the master "catch-all" block. If any unexpected error occurs during
        # the process, it will be caught here.
        logging.error(f"An unexpected error occurred in generate_fund_scorecard for scheme {scheme_code}: {e}", exc_info=True)
        # exc_info=True will add the full error traceback to the log for easy debugging.
        return {"error": "An unexpected internal error occurred during analysis. The development team has been notified."}

def calculate_returns_robust(nav_series: pd.Series) -> dict[str, float]:
    """
    A robust function to calculate returns for various periods.
    Its logic inherently avoids most errors by checking if data exists before calculation.
    """
    returns = {}
    nav_series = nav_series.sort_index()
    
    # Early exit if series is empty, preventing IndexError.
    if nav_series.empty:
        return {}

    latest_date = nav_series.index[-1]
    latest_nav = nav_series.iloc[-1]
    
    periods = {'1 Month': 1, '6 Months': 6, '1 Year': 12, '3 Years': 36}

    for name, months in periods.items():
        try:
            start_date = latest_date - pd.DateOffset(months=months)
            past_nav_series = nav_series[nav_series.index <= start_date]
            
            # This check prevents IndexErrors on the next line.
            if past_nav_series.empty:
                returns[name] = np.nan
                continue

            past_nav = past_nav_series.iloc[-1]
            
            # This check prevents ZeroDivisionErrors, though our pipeline should already filter these.
            if past_nav <= 0:
                returns[name] = np.nan
                continue
            
            num_years = months / 12.0
            total_return = (latest_nav / past_nav) - 1
            annualized_return = ((1 + total_return) ** (1 / num_years)) - 1
            returns[name] = annualized_return
        except Exception as e:
            # This block is a safety net for any unexpected numerical or date-related errors.
            logging.warning(f"Could not calculate return for period '{name}'. Reason: {e}")
            returns[name] = np.nan
            
    return returns

def calculate_sharpe_ratio(nav_series: pd.Series) -> float:
    """
    Calculates the annualized Sharpe Ratio with specific error handling
    for ZeroDivisionError.
    """
    if len(nav_series) < 252:
        return np.nan
        
    daily_returns = nav_series.pct_change().dropna()
    
    try:
        # This is the most likely point of failure. If all returns are the same,
        # the standard deviation will be 0, causing a ZeroDivisionError.
        std_dev = daily_returns.std()
        if std_dev == 0:
            # Handle the case of zero volatility explicitly. The risk-adjusted return is effectively zero.
            return 0.0

        daily_risk_free_rate = RISK_FREE_RATE / 252
        excess_returns = daily_returns - daily_risk_free_rate
        sharpe_ratio = (excess_returns.mean() / std_dev) * np.sqrt(252)
        return sharpe_ratio
        
    except Exception as e:
        # Catch any other unexpected numerical errors.
        logging.warning(f"Could not calculate Sharpe Ratio. Reason: {e}")
        return np.nan