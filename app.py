from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
from fastapi.middleware.cors import CORSMiddleware
import os
import re
from typing import Dict
from statistics import mean

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins, adjust this as needed
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all HTTP headers
)

# Set base path as constant
BASE_PATH = "clean/"

class SearchRequest(BaseModel):
    target_spend: float  # Target spend amount as a float
    carrier: str         # Carrier, e.g., 'UPS' or 'FedEx'
    tolerance: float     # Tolerance as a decimal (e.g., 0.2 for 20%)
    top_n: int          # Number of top service levels to return

def normalize_discount(discount: float) -> float:
    """Normalize discount value by dividing by 100 if it's greater than 100."""
    return discount / 100 if discount > 100 else discount

def parse_spend(spend_str: str) -> float:
    """Convert spend string from filename (like $670K or $2.2M) to float value."""
    spend_str = spend_str.replace('$', '').replace(',', '')
    if 'M' in spend_str:
        return float(spend_str.replace('M', '')) * 1_000_000
    elif 'K' in spend_str:
        return float(spend_str.replace('K', '')) * 1_000
    return float(spend_str)

def analyze_contracts(
    target_spend: float,
    carrier: str,
    tolerance: float,
    top_n: int
) -> Dict:
    """
    Analyze contracts to get statistics for each service level.

    Args:
        target_spend: Target spend amount as float
        carrier: 'UPS' or 'FedEx'
        tolerance: Tolerance range as decimal
        top_n: Number of top service levels to return

    Returns:
        Dictionary containing service level statistics
    """
    lower_spend = target_spend * (1 - tolerance)
    upper_spend = target_spend * (1 + tolerance)
    carrier_path = os.path.join(BASE_PATH, carrier)
    
    service_discounts = {}

    # Read all contract files in the carrier directory
    for filename in os.listdir(carrier_path):
        if filename.endswith('.csv'):
            spend_match = re.search(r'\$(.+?)\.csv', filename)
            if spend_match:
                contract_spend = parse_spend(spend_match.group(1))
                
                # Check if contract is within spend range
                if lower_spend <= contract_spend <= upper_spend:
                    df = pd.read_csv(os.path.join(carrier_path, filename))
                    current_col = f'CURRENT {carrier.upper()}'
                    
                    for _, row in df.iterrows():
                        service = row['DOMESTIC AIR SERVICE LEVEL']
                        try:
                            discount = normalize_discount(float(row[current_col]))
                            
                            if service not in service_discounts:
                                service_discounts[service] = []
                            service_discounts[service].append(discount)
                        except (ValueError, TypeError):
                            continue  # Skip invalid values
    
    service_stats = {}
    for service, discounts in service_discounts.items():
        if discounts:
            service_stats[service] = {
                'avg_discount': mean(discounts),
                'min_discount': min(discounts),
                'max_discount': max(discounts),
                'contract_count': len(discounts),
                'discount_values': sorted(discounts)
            }

    # Sort by average discount and get top N
    sorted_services = dict(sorted(
        service_stats.items(),
        key=lambda x: x[1]['avg_discount'],
        reverse=True
    )[:top_n])
    
    return sorted_services

@app.post("/analyze_contracts/")
async def analyze_contracts_endpoint(request: SearchRequest):
    """Endpoint to analyze contracts based on request parameters."""
    try:
        results = analyze_contracts(
            request.target_spend,
            request.carrier,
            request.tolerance,
            request.top_n
        )
        return format_results(results)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def format_results(stats: Dict) -> str:
    """Format results dictionary into the requested output string"""
    output = []
    
    for service, data in stats.items():
        # Multiply percentage values by 100 for display
        avg_discount = data['avg_discount'] * 100
        min_discount = data['min_discount'] * 100
        max_discount = data['max_discount'] * 100
        discount_values = [d * 100 for d in data['discount_values']]
        
        service_output = [
            f"\nService Level: {service}",
            f"Average Discount: {avg_discount:.2f}",
            f"Min Discount: {min_discount:.2f}",
            f"Max Discount: {max_discount:.2f}",
            f"Contract Count: {data['contract_count']}",
            f"Discount Values: {', '.join(f'{d:.2f}' for d in discount_values)}"
        ]
        output.extend(service_output)
    
    return "\n".join(output)

@app.get("/")
async def read_root():
    """Root endpoint for health check."""
    return {"message": "Welcome to the Contract Analysis API!"}

# Run the app using: uvicorn app:app --host 0.0.0.0 --port 8000 --reload
