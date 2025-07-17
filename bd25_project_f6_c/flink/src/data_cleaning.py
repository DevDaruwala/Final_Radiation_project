UNWANTED_COLUMNS = ['Location Name', 'MD5Sum', 'Height', 'Surface', 'Radiation', 'Loader ID']

def clean_record(record):
    try:
        unit = record.get('Unit', '').strip().lower()
        if unit != 'cpm':
            return None

        required_fields = ['Value', 'Captured Time', 'country']
        for field in required_fields:
            if field not in record or not str(record[field]).strip():
                return None

        try:
            float(record['Value'])  # Validate numeric
        except ValueError:
            return None

        # Clean unwanted fields
        cleaned_record = {k: v for k, v in record.items() if k not in UNWANTED_COLUMNS}
        return cleaned_record

    except Exception as e:
        print(f"Error in clean_record: {e}")
        return None
