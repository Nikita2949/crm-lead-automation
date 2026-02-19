"""
Excel → amoCRM Company Import Script

This script imports companies from an Excel file into amoCRM.
It maps Excel columns to amoCRM custom fields and automatically
adds a tag to each created company.

IMPORTANT:
Credentials must be stored in environment variables.
Do NOT hardcode secrets in this file.
"""

import os
import pandas as pd
from amocrm.v2 import Company as BaseCompany, Contact, tokens, custom_field


# ======================================================
# TOKEN CONFIGURATION (SECURE)
# ======================================================
# The following environment variables must be set:
# AMO_CLIENT_ID
# AMO_CLIENT_SECRET
# AMO_SUBDOMAIN
# AMO_REDIRECT_URL
# AMO_AUTH_CODE (used only once to initialize OAuth)

tokens.default_token_manager(
    client_id=os.getenv("AMO_CLIENT_ID"),
    client_secret=os.getenv("AMO_CLIENT_SECRET"),
    subdomain=os.getenv("AMO_SUBDOMAIN"),
    redirect_url=os.getenv("AMO_REDIRECT_URL"),
    storage=tokens.FileTokensStorage()
)

# Run this ONCE manually to initialize OAuth:
# tokens.default_token_manager.init(code=os.getenv("AMO_AUTH_CODE"))


# ======================================================
# Custom Company Model with amoCRM Custom Fields
# ======================================================

class Company(BaseCompany):
    """
    Extended Company model with custom fields mapping.
    Field IDs must match your amoCRM account configuration.
    """

    url = custom_field.UrlCustomField("URL", field_id=964013)
    work_hours = custom_field.TextCustomField("Working hours", field_id=964017)
    site = custom_field.UrlCustomField("Website", field_id=729369)
    phone = custom_field.ContactPhoneField("Phone", field_code="PHONE", enum_code="WORK")
    city = custom_field.TextCustomField("City", field_id=964357)
    district = custom_field.TextCustomField("District", field_id=964359)
    company_name = custom_field.TextCustomField("Company name", field_id=964361)


class ContactWithPhone(Contact):
    """
    Standard Contact model.
    (Extended if needed in future)
    """
    pass


# ======================================================
# Excel Import Logic
# ======================================================

def import_companies_from_excel(file_path: str):
    """
    Imports companies from an Excel file into amoCRM.

    Args:
        file_path (str): Path to the Excel file
    """

    # Load Excel file
    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return

    df.columns = df.columns.str.strip()
    print(f"Found {len(df)} records for import\n")

    success_count = 0
    error_count = 0

    for index, row in df.iterrows():
        try:
            company = Company()

            # ===============================
            # Mapping Excel columns → CRM
            # ===============================

            # Use address as company name if available
            if pd.notna(row.get('Адрес')):
                company.name = str(row['Адрес'])
            else:
                company.name = 'No address provided'

            if pd.notna(row.get('URL')):
                company.url = str(row['URL'])

            if pd.notna(row.get('Адрес')):
                company.address = str(row['Адрес'])

            if pd.notna(row.get('Сайт')):
                company.site = str(row['Сайт'])

            if pd.notna(row.get('Часы работы')):
                company.work_hours = str(row['Часы работы'])

            if pd.notna(row.get('Город')):
                company.city = str(row['Город'])

            if pd.notna(row.get('Район')):
                company.district = str(row['Район'])

            if pd.notna(row.get('Название')):
                company.company_name = str(row['Название'])

            if pd.notna(row.get('Телефон')):
                company.phone = str(row['Телефон'])

            # Add tag for tracking source
            company.tags.append("yandex_car_washing")

            # Save to amoCRM
            company.save()

            success_count += 1
            print(f"✓ [{index + 1}/{len(df)}] Company created (ID: {company.id})")

        except Exception as e:
            error_count += 1
            company_name = row.get('Адрес', 'Unknown')
            print(f"✗ [{index + 1}/{len(df)}] Error creating company '{company_name}': {str(e)}")
            continue

    # ===============================
    # Summary
    # ===============================
    print("\n" + "=" * 60)
    print("Import completed!")
    print(f"Successfully created: {success_count}")
    print(f"Errors: {error_count}")
    print(f"Total processed: {len(df)}")
    print("=" * 60)


# ======================================================
# ENTRY POINT
# ======================================================

if __name__ == "__main__":
    excel_file = "parsing_result/Moscow_final_result_27.12.25.xlsx"
    import_companies_from_excel(excel_file)
