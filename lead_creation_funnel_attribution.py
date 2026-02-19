"""
amoCRM Automation Script

This script processes companies in amoCRM by tag,
creates or updates leads, and ensures that each company
has an associated contact and active deal.

Main features:
- Exact tag filtering
- Automatic contact creation
- Lead creation or stage update
- Custom field mapping
- DRY RUN mode support
"""

import os
from amocrm.v2 import Company, Lead, Contact, tokens, custom_field


# ======================
# CONFIGURATION
# ======================

# IMPORTANT:
# Credentials must be stored in environment variables.
# Never hardcode secrets in public repositories.

tokens.default_token_manager(
    client_id=os.getenv("AMO_CLIENT_ID"),
    client_secret=os.getenv("AMO_CLIENT_SECRET"),
    subdomain=os.getenv("AMO_SUBDOMAIN"),
    redirect_url=os.getenv("AMO_REDIRECT_URL"),
    storage=tokens.FileTokensStorage()
)

# Initialize OAuth (run once manually)
# tokens.default_token_manager.init(code=os.getenv("AMO_AUTH_CODE"))


TAG_NAME = "yandex_car_washing"

PIPELINE_ID = 8397118
STATUS_ID = 68374590

DRY_RUN = False

# Status IDs considered as closed
CLOSED_STATUS_IDS = {142, 143}

# Mapping: Company custom field ID → Lead/Contact custom field ID
FIELD_MAPPING = {
    964013: 964089,  # URL
    964357: 964353,  # City
    964359: 964355,  # District
    729369: 964087,  # Website
    964017: 964095,  # Working hours
    964019: 964097,  # Phone
    964361: 964363,  # Company name
}


# ======================
# HELPER FUNCTIONS
# ======================

def company_has_exact_tag(company: Company, tag_name: str) -> bool:
    """
    Checks if a company contains an exact tag match.
    """
    try:
        if not hasattr(company, 'tags') or not company.tags:
            return False
        return any(tag.name.lower() == tag_name.lower() for tag in company.tags)
    except Exception as e:
        print(f"Tag check error: {e}")
        return False


def get_field_value(entity, field_id: int):
    """
    Extracts custom field value by field ID.
    """
    try:
        fields = entity._init_data.get("custom_fields_values") or []
        for field in fields:
            if field.get("field_id") == field_id:
                values = field.get("values") or []
                if values:
                    return values[0].get("value")
    except Exception as e:
        print(f"Field extraction error ({field_id}): {e}")
    return None


def get_open_lead_for_company(company: Company) -> Lead | None:
    """
    Returns the first open lead linked to the company.
    """
    try:
        if not hasattr(company, 'leads') or not company.leads:
            return None

        for lead_link in company.leads:
            lead = Lead.objects.get(object_id=lead_link.id)

            if lead.status.id not in CLOSED_STATUS_IDS:
                return lead

    except Exception as e:
        print(f"Lead search error: {e}")

    return None


def get_or_create_contact(company: Company) -> Contact | None:
    """
    Returns existing contact or creates a new one
    using company custom field data.
    """

    # Check existing contacts
    if hasattr(company, 'contacts') and company.contacts:
        contact = Contact.objects.get(object_id=list(company.contacts)[0].id)
        return contact

    # Create new contact
    full_company = Company.objects.get(object_id=company.id)

    contact = Contact(name=full_company.name)
    contact.responsible_user = full_company.responsible_user

    for company_field_id, contact_field_id in FIELD_MAPPING.items():
        value = get_field_value(full_company, company_field_id)
        if value:
            field = custom_field.TextCustomField("", field_id=contact_field_id)
            field.value = value
            contact.custom_fields.append(field)

    if not DRY_RUN:
        contact.save()

        # Link contact to company
        contact._init_data['_embedded'] = {
            'companies': [{'id': full_company.id}]
        }
        contact.save()

    return contact


def create_lead_with_contact(company: Company, contact: Contact) -> Lead | None:
    """
    Creates a new lead and links it to company and contact.
    """

    full_company = Company.objects.get(object_id=company.id)

    lead = Lead(name=f"Deal: {full_company.name}")
    lead.responsible_user = full_company.responsible_user

    # Copy custom fields
    for company_field_id, lead_field_id in FIELD_MAPPING.items():
        value = get_field_value(full_company, company_field_id)
        if value:
            field = custom_field.TextCustomField("", field_id=lead_field_id)
            field.value = value
            lead.custom_fields.append(field)

    lead._init_data['pipeline_id'] = PIPELINE_ID
    lead._init_data['status_id'] = STATUS_ID

    if not DRY_RUN:
        lead.save()

        lead._init_data['_embedded'] = {
            'companies': [{'id': full_company.id}],
            'contacts': [{'id': contact.id}] if contact else []
        }

        lead.save()

    return lead


def move_lead_to_stage(lead: Lead):
    """
    Moves an existing lead to target pipeline stage.
    """
    lead._init_data['pipeline_id'] = PIPELINE_ID
    lead._init_data['status_id'] = STATUS_ID

    if not DRY_RUN:
        lead.save()


# ======================
# MAIN WORKFLOW
# ======================

def main():
    """
    Main execution flow:
    - Load all companies
    - Filter by tag
    - Ensure contact exists
    - Create or update lead
    """

    print("Starting amoCRM automation...")
    print(f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}")

    processed = 0
    created = 0
    updated = 0

    all_companies = list(Company.objects.all())

    companies_with_tag = [
        comp for comp in all_companies
        if company_has_exact_tag(comp, TAG_NAME)
    ]

    print(f"Companies with tag '{TAG_NAME}': {len(companies_with_tag)}")

    for company in companies_with_tag:
        processed += 1
        print(f"\nProcessing company {company.id} — {company.name}")

        contact = get_or_create_contact(company)
        lead = get_open_lead_for_company(company)

        if lead:
            move_lead_to_stage(lead)
            updated += 1
        else:
            create_lead_with_contact(company, contact)
            created += 1

    print("\nSummary:")
    print(f"Processed: {processed}")
    print(f"Created leads: {created}")
    print(f"Updated leads: {updated}")
    print("Done.")


if __name__ == "__main__":
    main()
