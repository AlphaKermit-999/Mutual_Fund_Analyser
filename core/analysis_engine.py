import pandas as pd
import numpy as np
from config import RISK_FREE_RATE
from .database import get_nav_history_by_code

def generate_fund_scorecard(scheme_code: int) -> dict:
    """Orchestrates all calculations and generates the final scorecard for a given fund."""
    nav_history = get_nav_history_by_code(scheme_code)
    
    if nav_history.empty or 'nav' not in nav_history.columns:
        return {"error": "NAV data is empty or invalid for this fund."}
        
    nav_series = nav_history['nav']
    
    returns = _calculate_returns(nav_series)
    sharpe = _calculate_sharpe_ratio(nav_series)
    volatility = nav_series.pct_change().std() * np.sqrt(252)

    score_components = {}
    perf_score = 0
    if returns.get('1 Year', -1) > 0.20: perf_score += 25
    elif returns.get('1 Year', -1) > 0.10: perf_score += 10
    if returns.get('3 Years', -1) > 0.15: perf_score += 25
    elif returns.get('3 Years', -1) > 0.10: perf_score += 15
    score_components['Performance'] = perf_score
    
    risk_score = 0
    if sharpe > 1.0: risk_score = 50
    elif sharpe > 0.75: risk_score = 35
    elif sharpe > 0.5: risk_score = 20
    score_components['Risk-Adjusted Return'] = risk_score
    
    final_score = sum(score_components.values())
    
    return {
        "metrics": {**returns, "sharpe_ratio": sharpe, "annual_volatility": volatility},
        "scores": {"components": score_components, "final_score": final_score}
    }

def _calculate_returns(nav_series: pd.Series) -> dict[str, float]:
    """Helper function to calculate returns for various periods."""
    daily_nav = nav_series.resample('D').ffill()
    returns = {}
    periods = {'1 Month': 30, '6 Months': 182, '1 Year': 365, '3 Years': 365*3}
    for name, days in periods.items():
        if len(daily_nav) > days:
            ret = (daily_nav.iloc[-1] / daily_nav.iloc[-days-1]) - 1
            returns[name] = ((1 + ret) ** (365.0 / days) - 1) if days >= 365 else ret
        else:
            returns[name] = np.nan
    return returns

def _calculate_sharpe_ratio(nav_series: pd.Series) -> float:
    """Helper function to calculate the annualized Sharpe Ratio."""
    if len(nav_series) < 252: return np.nan
    daily_returns = nav_series.pct_change().dropna()
    excess_returns = daily_returns - (RISK_FREE_RATE / 252)
    return (excess_returns.mean() / excess_returns.std()) * np.sqrt(252)