
import argparse
import apache_beam as beam
from apache_beam import PCollection

from apache_beam.options.pipeline_options import PipelineOptions

def main():
    parser = argparse.ArgumentParser(description="Nuestro primer pipeline")
    parser.version="1.0"
    parser.add_argument("--entrada", help="Fichero de entrada")
    parser.add_argument("--salida", help="Fichero de salida")

    our_args, beam_args = parser.parse_known_args()
    run_pipeline(our_args, beam_args)

def run_pipeline(custom_args,beam_args):
    entrada = custom_args.entrada
    salida = custom_args.salida

    opts = PipelineOptions(beam_args)

    #creando popeline
    with beam.Pipeline(options=opts) as p:
        #leer fichero de entrada
        lineas_texto: PCollection[str] = p | beam.io.ReadFromText(entrada)
        # "En un lugar de la mancha" --> ["En","un"....]
        palabras =  lineas_texto | beam.FlatMap(lambda l: l.split())

        #contar palabras
        # "En" --> ("En", 17)
        # "un" --> ("un", 20)
        contadas: PCollection[Tuple[str,int]] = palabras | beam.combiners.Count.PerElement()
        
        #Top 25 palabras
        palabras_top = contadas | beam.combiners.Top.Of(5, key=lambda kv: kv[1])
        palabras_top | beam.Map(print)

if __name__ == "__main__":
    main()