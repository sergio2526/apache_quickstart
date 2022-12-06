
import argparse
import apache_beam as beam
from apache_beam import PCollection

from apache_beam.options.pipeline_options import PipelineOptions

#Corrigiendo palabras mediante unas reglas
def sanitizar_palabra(palabra):
    para_quitar = [",",".","-",":"," "]

    for simbolo in para_quitar:
        palabra = palabra.replace(simbolo, "")

    palabra = palabra.lower()

    palabra = palabra.replace("á", "a")
    palabra = palabra.replace("é", "e")
    palabra = palabra.replace("í", "i")
    palabra = palabra.replace("ó", "o")
    palabra = palabra.replace("ú", "u")

    return palabra

def main():
    parser = argparse.ArgumentParser(description="Nuestro primer pipeline")
    parser.version="1.0"
    parser.add_argument("--entrada", help="Fichero de entrada")
    parser.add_argument("--salida", help="Fichero de salida")
    parser.add_argument("--n-palabras", type=int, help="Numero de palabras en la salida")

    our_args, beam_args = parser.parse_known_args()
    run_pipeline(our_args, beam_args)

def run_pipeline(custom_args,beam_args):
    entrada = custom_args.entrada
    salida = custom_args.salida
    n_palabras = custom_args.n_palabras

    opts = PipelineOptions(beam_args)

    #creando popeline
    with beam.Pipeline(options=opts) as p:
        #leer fichero de entrada
        lineas_texto: PCollection[str] = p | beam.io.ReadFromText(entrada)
        # "En un lugar de la mancha" --> ["En","un"....]
        palabras =  lineas_texto | beam.FlatMap(lambda l: l.split())
        palabras_limpiadas = palabras | beam.Map(sanitizar_palabra)
        #contar palabras
        # "En" --> ("En", 17)
        # "un" --> ("un", 20)
        contadas: PCollection[Tuple[str,int]] = palabras_limpiadas | beam.combiners.Count.PerElement()
        
        #Top 25 palabras
        palabras_top_lista = contadas | beam.combiners.Top.Of(n_palabras, key=lambda kv: kv[1])
        palabras_top = palabras_top_lista | beam.FlatMap(lambda x: x) #sacando palabras
        formateado: PCollection[str] = palabras_top  | beam.Map(lambda kv: "%s,%d" % (kv[0], kv[1])) #Formateando salida
        
        formateado | beam.io.WriteToText(salida, file_name_suffix=".csv")

if __name__ == "__main__":
    main()