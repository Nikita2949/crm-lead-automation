from amocrm.v2 import Company, Lead, Contact, tokens, custom_field

# ======================
# НАСТРОЙКИ
# ======================

tokens.default_token_manager(
    client_id="18d15742-4c01-424f-ab14-43abb4bc0fb0",
    client_secret="AP9xbkWYkF956MfC1o4tGkRoSRGdVHBTrhVIW3LOAY7CYeDVQyEfGVdMl8a2saLp",
    subdomain="servispro",
    redirect_url="https://ya.ru",
    storage=tokens.FileTokensStorage()
)

tokens.default_token_manager.init(
    code="def50200f590c358efe510b9e65d09ecfb0134045d599e2bd3b9e4b97e8c462d4011f861b2132c548ea79795fb78172638c66abf2e71aa57a27c230799345be6f5956a55bc55017fdd233116edc603ebba26b5e12460486ff18fc797a80ad23792a93fe680df1b8aac09b26cf6dbe2d8eaa5b7523afb5ce6d622512d60c0f5825d9b900a6d84c5708f35d8c2aaea3f7bfe8d3739043b436ba6410d59a56ae060f9f2c3dc162d34b011938653b981360323762ea74cb5781fe1ea227f3d2267f7b87cd6557d2f40eb4c6ecc078d5496832d23734191673ad66a761c43d9d91260bda4b7e1d5355a5ddfed593a722237e50be5a5722b4206e298ab7486a14703328bd570bafb4cf0feb55e869fe820e3fc19305e882c6fbc129bc400acc87cdf1a01b61220ea44c343ce261bf3e930e7a59dcc427147435fc95c4da8f3c99f80f21b876109df63e2c350b1ffe03f422c518f347bbfe6c9251eaad2ad6ac0a63a5a7ab5df3bc0d726ebbd2b67e5b1ec3279033be2cb7c982adc6f8849a9531a863dbcbc6b9b72a37c8b73a495296838bbebac7195ae42fbffda0982e1411722dca0aa87214469aafe1404fc46eab1605434884ee341c82e468fd909a7adc8ef3f06e6080eeb208439201be2677b5faa586553a83d67db2d8c36bd7ee6ad49a7"
)

# ======================
# ПАРАМЕТРЫ
# ======================

TAG_NAME = "yandex_car_washing"
PIPELINE_ID = 8397118
STATUS_ID = 68374590
DRY_RUN = False
CLOSED_STATUS_IDS = {142, 143}

# Маппинг полей: ID компании → ID контакта/лида
FIELD_MAPPING = {
    964013: 964089,  # URL
    964357: 964353,  # Город
    964359: 964355,  # Район
    729369: 964087,  # Web
    964017: 964095,  # Часы работы
    964019: 964097,  # Телефон (если есть)
    964361: 964363,  # Название
}

# ======================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ======================

def company_has_exact_tag(company: Company, tag_name: str) -> bool:
    """Проверяет точное совпадение тега"""
    try:
        if not hasattr(company, 'tags') or not company.tags:
            return False
        return any(tag.name.lower() == tag_name.lower() for tag in company.tags)
    except Exception as e:
        print(f"    ⚠ Ошибка проверки тега: {e}")
        return False

def get_field_value(entity, field_id: int):
    """Получает значение поля по ID"""
    try:
        fields = entity._init_data.get("custom_fields_values") or []
        for field in fields:
            if field.get("field_id") == field_id:
                values = field.get("values") or []
                if values:
                    return values[0].get("value")
    except Exception as e:
        print(f"    ⚠ Ошибка получения поля {field_id}: {e}")
    return None

def get_open_lead_for_company(company: Company) -> Lead | None:
    """Возвращает первую открытую сделку компании или None"""
    try:
        if not hasattr(company, 'leads') or not company.leads:
            return None
            
        for lead_link in company.leads:
            try:
                lead = Lead.objects.get(object_id=lead_link.id)
                if hasattr(lead, 'status') and hasattr(lead.status, 'id'):
                    if lead.status.id not in CLOSED_STATUS_IDS:
                        return lead
            except Exception as e:
                print(f"    ⚠ Ошибка получения лида {lead_link.id}: {e}")
                continue
    except Exception as e:
        print(f"    ⚠ Ошибка поиска лидов: {e}")
    return None

def get_or_create_contact(company: Company) -> Contact | None:
    """Возвращает существующий контакт или создает новый с данными компании"""
    
    # Проверяем существующие контакты
    try:
        if hasattr(company, 'contacts') and company.contacts:
            contacts_list = list(company.contacts)
            if contacts_list:
                try:
                    contact = Contact.objects.get(object_id=contacts_list[0].id)
                    print(f"    → Используется существующий контакт {contact.id}")
                    return contact
                except Exception as e:
                    print(f"    ⚠ Не удалось загрузить контакт: {e}")
    except Exception as e:
        print(f"    ⚠ Ошибка проверки контактов: {e}")
    
    # Создаем новый контакт
    print(f"    → Создание нового контакта...")
    
    try:
        # Перезагружаем компанию с полными данными
        full_company = Company.objects.get(object_id=company.id)
        
        print(f"    → Доступные поля компании:")
        fields = full_company._init_data.get("custom_fields_values") or []
        if not fields:
            print(f"       Нет кастомных полей")
        else:
            for field in fields:
                field_id = field.get("field_id")
                values = field.get("values") or []
                value = values[0].get("value") if values else None
                if value:
                    print(f"       {field_id}: {str(value)[:60]}")
        
        # Создаем контакт
        contact = Contact(name=full_company.name)
        contact.responsible_user = full_company.responsible_user
        
        # Копируем кастомные поля
        fields_copied = 0
        for company_field_id, contact_field_id in FIELD_MAPPING.items():
            value = get_field_value(full_company, company_field_id)
            if value:
                try:
                    field = custom_field.TextCustomField("", field_id=contact_field_id)
                    field.value = value
                    contact.custom_fields.append(field)
                    fields_copied += 1
                    print(f"       ✓ Скопировано поле {company_field_id} → {contact_field_id}")
                except Exception as e:
                    print(f"       ✗ Ошибка копирования {company_field_id}: {e}")
        
        if not DRY_RUN:
            # Сохраняем контакт
            contact.save()
            print(f"    ✔ Контакт {contact.id} создан ({fields_copied} полей)")
            
            # Привязываем к компании ПОСЛЕ создания
            try:
                # Обновляем контакт с привязкой к компании
                contact._init_data['_embedded'] = {
                    'companies': [{'id': full_company.id}]
                }
                contact.save()
                print(f"    ✔ Контакт привязан к компании {full_company.id}")
            except Exception as e:
                print(f"    ⚠ Не удалось привязать к компании: {e}")
        else:
            print(f"    [DRY RUN] Контакт с {fields_copied} полями")
        
        return contact
        
    except Exception as e:
        print(f"    ✗ Ошибка создания контакта: {e}")
        import traceback
        traceback.print_exc()
        return None

def create_lead_with_contact(company: Company, contact: Contact) -> Lead | None:
    """Создаёт лид с привязкой компании и контакта, копирует поля"""
    try:
        # Перезагружаем компанию для получения всех данных
        full_company = Company.objects.get(object_id=company.id)
        
        lead = Lead(name=f"Сделка: {full_company.name}")
        lead.responsible_user = full_company.responsible_user
        
        # Копируем поля из компании в лид
        fields_copied = 0
        for company_field_id, lead_field_id in FIELD_MAPPING.items():
            value = get_field_value(full_company, company_field_id)
            if value:
                try:
                    field = custom_field.TextCustomField("", field_id=lead_field_id)
                    field.value = value
                    lead.custom_fields.append(field)
                    fields_copied += 1
                except Exception as e:
                    print(f"    ⚠ Ошибка копирования поля в лид: {e}")
        
        if fields_copied > 0:
            print(f"    → Скопировано {fields_copied} полей в лид")
        
        # Устанавливаем воронку и статус
        lead._init_data['pipeline_id'] = PIPELINE_ID
        lead._init_data['status_id'] = STATUS_ID
        
        if not DRY_RUN:
            # Сохраняем лид
            lead.save()
            print(f"    ✔ Лид {lead.id} создан (воронка {PIPELINE_ID}, статус {STATUS_ID})")
            
            # Привязываем компанию и контакт
            try:
                lead._init_data['_embedded'] = {
                    'companies': [{'id': full_company.id}]
                }
                
                if contact and hasattr(contact, 'id'):
                    lead._init_data['_embedded']['contacts'] = [{'id': contact.id}]
                
                lead.save()
                contact_info = f", контакт {contact.id}" if contact else ""
                print(f"    ✔ Привязаны: компания {full_company.id}{contact_info}")
                
            except Exception as e:
                print(f"    ⚠ Ошибка привязки: {e}")
            
            return lead
        else:
            print(f"    [DRY RUN] Лид будет создан с {fields_copied} полями")
            return lead
            
    except Exception as e:
        print(f"    ✗ Ошибка создания лида: {e}")
        import traceback
        traceback.print_exc()
        return None

def move_lead_to_stage(lead: Lead):
    """Перемещает лид в нужную воронку и этап"""
    try:
        lead._init_data['pipeline_id'] = PIPELINE_ID
        lead._init_data['status_id'] = STATUS_ID
        
        if not DRY_RUN:
            lead.save()
            print(f"    ✔ Лид {lead.id} перемещен")
        else:
            print(f"    [DRY RUN] Лид будет перемещен")
            
    except Exception as e:
        print(f"    ✗ Ошибка перемещения: {e}")

# ======================
# ОСНОВНАЯ ЛОГИКА
# ======================

def main():
    print("="*60)
    print(f"РЕЖИМ: {'🔍 DRY RUN' if DRY_RUN else '▶️ ВЫПОЛНЕНИЕ'}")
    print(f"Тег: '{TAG_NAME}' (точное совпадение)")
    print(f"Воронка: {PIPELINE_ID}, Статус: {STATUS_ID}")
    print("="*60)
    print()
    
    processed = 0
    created_leads = 0
    updated_leads = 0
    created_contacts = 0
    skipped = 0
    errors = 0

    try:
        # Получаем ВСЕ компании
        all_companies = list(Company.objects.all())
        print(f"Загружено компаний: {len(all_companies)}")
        
        # Фильтруем по тегу вручную
        companies_with_tag = []
        for comp in all_companies:
            if company_has_exact_tag(comp, TAG_NAME):
                companies_with_tag.append(comp)
        
        print(f"С тегом '{TAG_NAME}': {len(companies_with_tag)}\n")
        
    except Exception as e:
        print(f"✗ Ошибка загрузки компаний: {e}")
        return

    for company in companies_with_tag:
        processed += 1
        print(f"\n{'─'*60}")
        print(f"[{processed}] Компания {company.id} — {company.name}")
        
        # Показываем теги
        try:
            tags = [tag.name for tag in company.tags] if company.tags else []
            print(f"    Теги: {', '.join(tags)}")
        except:
            pass
        
        print(f"{'─'*60}")

        try:
            # Получаем или создаем контакт
            contact = get_or_create_contact(company)
            if contact and hasattr(contact, 'id') and not DRY_RUN:
                created_contacts += 1
            
            # Проверяем существующие лиды
            lead = get_open_lead_for_company(company)

            if lead:
                print(f"[UPDATE] Найден открытый лид {lead.id}")
                move_lead_to_stage(lead)
                updated_leads += 1
            else:
                print(f"[CREATE] Создание лида...")
                new_lead = create_lead_with_contact(company, contact)
                if new_lead:
                    created_leads += 1

        except Exception as e:
            errors += 1
            print(f"\n[ERROR] Компания {company.id}")
            print(f"Ошибка: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            print("\nПродолжаем...\n")

    # Итоги
    print(f"\n{'='*60}")
    print(f"ИТОГИ:")
    print(f"{'='*60}")
    print(f"Обработано компаний:  {processed}")
    print(f"Создано лидов:        {created_leads}")
    print(f"Обновлено лидов:      {updated_leads}")
    print(f"Создано контактов:    {created_contacts}")
    print(f"Пропущено:            {skipped}")
    print(f"Ошибок:               {errors}")
    print(f"{'='*60}")
    print("✅ Готово!")

if __name__ == "__main__":
    main()