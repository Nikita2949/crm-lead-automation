import pandas as pd
from amocrm.v2 import Company as BaseCompany, Contact, tokens, custom_field

# --- Настройка токена ---
tokens.default_token_manager(
    client_id="18d15742-4c01-424f-ab14-43abb4bc0fb0",
    client_secret="AP9xbkWYkF956MfC1o4tGkRoSRGdVHBTrhVIW3LOAY7CYeDVQyEfGVdMl8a2saLp",
    subdomain="servispro",
    redirect_url="https://ya.ru",
    storage=tokens.FileTokensStorage()
)

tokens.default_token_manager.init(
    code="def5020055f53512989872a168a31fb41c1fc5db53e1e2f42388618d6347be30fba5f85bfc18b49e9fac1006691ac645672a36e5f3b05bd93b3595e9a4d5cb2979224be7f7ebac0b1ee842bafd6bed83ff3fb101a0c42ffd1b213502b23158c1ea3a8cdb5cd86d0c5f3bed7542949f4b5b85d053ed80add94b4910bcc9cc43622aed0e671dfdef055e76c283dc55e430ed87183c3a3c878e673bc8a2dbde9138afced7d9bc6e47874d7aca6df1929efc124384bb4864400bf266071c70c4ee053eedb540692d213f933988592e0818aa489f54806feeda1a372721117c1b18a9105c713f9508c9a7fec020962f76d638852e73db593b8d0748eef29548fd226eed01c6cc61d45feaa1daabe52041e7fbe932eca2a4459ebcef3f4ecf9a6095b04a35dfc592f4c769de50523cc784f9380072b579a16bb9732ee502d018daaf86229a617408021a2e543716cd7e5aa8c6e31373aa99cf5e96429a0d7e48695ac6c9c065026d58b67e680f42f9520d979e3448b6869a345c6ea1c1df54bd620b91d6770eb22fc2f034edbf63d765c128bb8865157de69c80f3518841b1774d0374eabab9d3613ae99cd64b77520de223055f348a2351565233d1ed8c46affb80e7e3ebafe8b30fb978edc8e95da3e1b547e8a4dd485320a3d96fdd3500d33d"
)


# --- Класс Company ---
class Company(BaseCompany):
    url = custom_field.UrlCustomField("URL", field_id=964013)
    work_hours = custom_field.TextCustomField("часы работы", field_id=964017)
    site = custom_field.UrlCustomField("Web", field_id=729369)
    phone = custom_field.ContactPhoneField("Телефон", field_code="PHONE", enum_code="WORK")
    city = custom_field.TextCustomField("Город", field_id=964357)
    district = custom_field.TextCustomField("Район", field_id=964359)
    company_name = custom_field.TextCustomField("Название", field_id=964361)


# --- Класс Contact с телефоном ---
class ContactWithPhone(Contact):
    pass  # Используем стандартные поля Contact


def import_companies_from_excel(file_path):
    """
    Импортирует компании из Excel файла в AmoCRM
    
    Args:
        file_path: путь к Excel файлу
    """
    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")
        return

    df.columns = df.columns.str.strip()
    print(f"Найдено {len(df)} записей для импорта\n")

    success_count = 0
    error_count = 0

    for index, row in df.iterrows():
        try:
            # --- Создание компании ---
            company = Company()
            
            # Используем адрес в названии компании
            if pd.notna(row.get('Адрес')):
                company.name = str(row['Адрес'])
            else:
                company.name = 'Без адреса'

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

            # Телефон
            if pd.notna(row.get('Телефон')):
                company.phone = str(row['Телефон'])

            # Добавление тега
            company.tags.append("yandex_car_washing")

            company.save()

            success_count += 1
            print(f"✓ [{index + 1}/{len(df)}] Компания '{company.company_name}' успешно создана (ID: {company.id})")

        except Exception as e:
            error_count += 1
            company_name = row.get('Адрес', 'Неизвестно') if isinstance(row.get('Адрес'), str) else 'Неизвестно'
            print(f"✗ [{index + 1}/{len(df)}] Ошибка при создании компании '{company_name}': {str(e)}")
            continue

    print(f"\n{'='*60}")
    print(f"Импорт завершён!")
    print(f"Успешно создано: {success_count}")
    print(f"Ошибок: {error_count}")
    print(f"Всего обработано: {len(df)}")
    print(f"{'='*60}")


if __name__ == "__main__":
    # Укажите путь к вашему Excel файлу
    excel_file = "parsing_result/Moscow_final_result_27.12.25.xlsx"
    
    # Запуск импорта
    import_companies_from_excel(excel_file)