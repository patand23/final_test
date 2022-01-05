import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions



def run():
    pipeline_options = PipelineOptions(
        project='york-cdf-start',
        region='us-central1',
        temp_location='gs://pas_df_bucket/dataflow/tmp/',
        staging_location='gs://pas_df_bucket/dataflow/staging/',
        save_main_session=True
    )

    with beam.Pipeline(options=pipeline_options) as p:
        table_eur = p | 'Read in from Table1' >> beam.io.ReadFromBigQuery(table='york-cdf-start:pas_df.eur_order_payment_history')
        table_usd = p | 'Read in from Table2' >> beam.io.ReadFromBigQuery(table='york-cdf-start:pas_df.usd_order_payment_history')
        table_gbp = p | 'Read in from Table2' >> beam.io.ReadFromBigQuery(table='york-cdf-start:pas_df.gbp_order_payment_history')

if __name__ == "__main__":
    run()