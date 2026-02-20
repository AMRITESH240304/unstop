from typing_extensions import TypedDict, Annotated
from langchain.messages import AnyMessage
from langchain_core.messages import SystemMessage, HumanMessage
from langgraph.graph import StateGraph, START, END
from langchain_google_genai import ChatGoogleGenerativeAI
from config import settings
import operator
import json
import re

model = ChatGoogleGenerativeAI(
    model="gemini-flash-latest",
    temperature=0.2,
    google_api_key=settings.GEMINI_API_KEY,
)

class InvoiceState(TypedDict):
    messages: Annotated[list[AnyMessage], operator.add]
    invoice_text: str
    extracted_data: dict
    validation_result: dict

def parse_llm_json(response):
    content = response.content

    # Gemini may return list
    if isinstance(content, list):
        content = "".join(part.get("text", "") for part in content)

    content = content.strip()
    content = re.sub(r"```json", "", content)
    content = re.sub(r"```", "", content)

    match = re.search(r"\{.*\}", content, re.DOTALL)
    if match:
        content = match.group(0)

    return json.loads(content)

def extract_fields_node(state: InvoiceState):

    prompt = f"""
You are an invoice extraction system.

Extract the following fields from this invoice text:

- invoice_number
- invoice_date
- vendor_name
- line_items (description, quantity, unit_price, total)
- subtotal
- tax
- total

Return ONLY valid JSON.

Invoice text:
{state["invoice_text"]}
"""

    response = model.invoke([
        SystemMessage(content="You extract structured invoice data."),
        HumanMessage(content=prompt)
    ])

    extracted_data = parse_llm_json(response)

    return {
        "extracted_data": extracted_data
    }


def calculation_check_node(state: InvoiceState):

    data = state.get("extracted_data", {})
    errors = []
    computed_subtotal = 0.0

    for item in data.get("line_items", []):
        try:
            quantity = float(item.get("quantity", 0))
            unit_price = float(item.get("unit_price", 0))
            total = float(item.get("total", 0))

            expected_total = quantity * unit_price

            if abs(expected_total - total) > 0.01:
                errors.append(
                    f"Line item mismatch: {item.get('description', 'unknown item')}"
                )

            computed_subtotal += total

        except Exception:
            errors.append("Invalid numeric value in line item")

    subtotal = float(data.get("subtotal", 0))
    tax = float(data.get("tax", 0))
    final_total = float(data.get("total", 0))

    if abs(computed_subtotal - subtotal) > 0.01:
        errors.append("Subtotal mismatch")

    if abs((subtotal + tax) - final_total) > 0.01:
        errors.append("Final total mismatch")

    return {
        "validation_result": {
            "calculation_errors": errors
        }
    }

def missing_fields_node(state: InvoiceState):

    data = state.get("extracted_data", {})

    prompt = f"""
Check if any required invoice fields are missing.

Required fields:
- invoice_number
- invoice_date
- vendor_name
- subtotal
- tax
- total

Extracted data:
{json.dumps(data)}

Return ONLY valid JSON:
{{
  "missing_fields": [],
  "is_valid": true/false
}}
"""

    response = model.invoke([
        SystemMessage(content="You validate invoice completeness."),
        HumanMessage(content=prompt)
    ])

    result = parse_llm_json(response)

    final_result = {
        **state.get("validation_result", {}),
        **result
    }

    return {
        "validation_result": final_result
    }

builder = StateGraph(InvoiceState)

builder.add_node("extract_fields", extract_fields_node)
builder.add_node("calculation_check", calculation_check_node)
builder.add_node("missing_fields", missing_fields_node)

builder.add_edge(START, "extract_fields")
builder.add_edge("extract_fields", "calculation_check")
builder.add_edge("calculation_check", "missing_fields")
builder.add_edge("missing_fields", END)

invoice_graph = builder.compile()

def process_invoice(invoice_text: str):

    result = invoice_graph.invoke({
        "invoice_text": invoice_text,
        "messages": []
    })

    # ✅ NEW FINAL STRUCTURE
    return {
        "invoice_data": result.get("extracted_data", {}),
        "validation": result.get("validation_result", {})
    }