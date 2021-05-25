#new v3beta1 api for GCP document translation
from google.cloud import translate_v3beta1 as translate

#enter the project ID and the location
PROJECT_ID = ""
LOCATION) = "us-central1"

parent = "projects/{}/locations/{}".format(PROJECT_ID, LOCATION)

#update the input file location from the GCS bucket/folder
input_config = translate.types.DocumentInputConfig(mime_type="application/pdf",
                                             gcs_source=translate.GcsSource(input_uri = "gs://bharath_test/sample (1).pdf"))

#give the location where the new file should be uploaded to
output_config = translate.types.DocumentOutputConfig(mime_type="application/pdf",
                                             gcs_destination=translate.GcsDestination(output_uri_prefix = "gs://bharath_test/testfile/"))

#create a translation document request
doc_request = translate.TranslateDocumentRequest(parent = parent,
                                            source_language_code='en-US',
                                            target_language_code="es",
                                            document_input_config=input_config,
                                            document_output_config=output_config)

client = translate.TranslationServiceClient()
response = client.translate_document(request=doc_request)
