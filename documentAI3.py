from google.cloud import documentai_v1beta2 as documentai
import json
from ast import literal_eval
from google.cloud import bigquery

def parse_form(project_id='quantiphi-ttest',
               input_uri='gs://document_ai1/Payslip_11176322 (2).pdf'):
    """Parse a form"""

    client = documentai.DocumentUnderstandingServiceClient()

    gcs_source = documentai.types.GcsSource(uri=input_uri)

    # mime_type can be application/pdf, image/tiff,
    # and image/gif, or application/json
    input_config = documentai.types.InputConfig(
        gcs_source=gcs_source, mime_type='application/pdf')

    # Improve form parsing results by providing key-value pair hints.
    # For each key hint, key is text that is likely to appear in the
    # document as a form field name (i.e. "DOB").
    # Value types are optional, but can be one or more of:
    # ADDRESS, LOCATION, ORGANIZATION, PERSON, PHONE_NUMBER, ID,
    # NUMBER, EMAIL, PRICE, TERMS, DATE, NAME
    key_value_pair_hints = [
        documentai.types.KeyValuePairHint(key='Personnel No',
                                          ),
        documentai.types.KeyValuePairHint(
            key='Name', value_types=['NAME']),

        documentai.types.KeyValuePairHint(
            key='Bank'),

        documentai.types.KeyValuePairHint(
            key='Bank A/c No'),

        documentai.types.KeyValuePairHint(
            key='DOJ'),

        documentai.types.KeyValuePairHint(
            key='LOP Days'),

        documentai.types.KeyValuePairHint(
            key='PF No.'),

        documentai.types.KeyValuePairHint(
            key='Location'),

        documentai.types.KeyValuePairHint(
            key='Facility'),

        documentai.types.KeyValuePairHint(
            key='Department'),

        documentai.types.KeyValuePairHint(
            key='INCOME TAX'),

        documentai.types.KeyValuePairHint(
            key='PROFESSIONAL TAX'),

        documentai.types.KeyValuePairHint(
            key='GROSS DEDUCTIONS'),

        documentai.types.KeyValuePairHint(
            key='PROVIDENT FUND'),

        documentai.types.KeyValuePairHint(
            key='NGO CONTRIBUTION'),

        documentai.types.KeyValuePairHint(
            key='PF – UAN'),
    ]

    # Setting enabled=True enables form extraction
    form_extraction_params = documentai.types.FormExtractionParams(
        enabled=True, key_value_pair_hints=key_value_pair_hints)

    # Location can be 'us' or 'eu'
    parent = 'projects/{}/locations/us'.format(project_id)
    request = documentai.types.ProcessDocumentRequest(
        parent=parent,
        input_config=input_config,
        form_extraction_params=form_extraction_params)

    document = client.process_document(request=request)

    def _get_text(el):
        """Doc AI identifies form fields by their offsets
        in document text. This function converts offsets
        to text snippets.
        """
        response = ''
        # If a text segment spans several lines, it will
        # be stored in different text segments.
        for segment in el.text_anchor.text_segments:
            start_index = segment.start_index
            end_index = segment.end_index
            response += document.text[start_index:end_index]
        return response

    jsonDict = {}
    for page in document.pages:
        print('Page number: {}'.format(page.page_number))
        for form_field in page.form_fields:
            # fieldNames.append(_get_text(form_field.field_name))
            print('Field Name: {}\tConfidence: {}'.format(
                _get_text(form_field.field_name),
                form_field.field_name.confidence))

            # fieldValues.append(_get_text(form_field.field_value))
            print('Field Value: {}\tConfidence: {}'.format(
                _get_text(form_field.field_value),
                form_field.field_value.confidence))

            jsonDict[_get_text(form_field.field_name) \
            .strip() \
            .replace('PF \u2013 UAN', 'UAN')] = _get_text(form_field.field_value) \
                                                        .replace('\n','') \
                                                        .strip()


    print(json.dumps(jsonDict))

    client = bigquery.Client()
    filename = '/path/to/file/in/nd-format.json'
    dataset_id = 'quantiphi'
    table_id = 'dataLoad'

parse_form()
