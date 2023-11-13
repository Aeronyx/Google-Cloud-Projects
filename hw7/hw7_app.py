# George Trammell
import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions

# uses bs4 to extract links from pages (similar to hw2)
def parse_html(file):
    from bs4 import BeautifulSoup
    # import must be inside function or dataflow will crash
    content = file.read_utf8()
    soup = BeautifulSoup(content, 'html.parser')
    page = file.metadata.path.split('/')[-1]

    links = [link.get('href') for link in soup.find_all('a')]
    return page, links

# pipeline options object for readability
options = PipelineOptions(flags=[], **{
    'runner': 'DirectRunner',
    'project': 'robust-builder-398218',
    'staging_location': 'gs://ds561-hw2-html-linked-bucket/staging',
    'temp_location': 'gs://ds561-hw2-html-linked-bucket/temp',
    'region': 'us-east1',
    'requirements_file': './requirements.txt'
})

# main beam app code
with beam.Pipeline(options=options) as pipeline:

    # read and extract outgoing links
    parsed_files = (pipeline 
                    | 'MatchHTMLFiles' >> fileio.MatchFiles('gs://ds561-hw2-html-linked-bucket/html/*.html')
                    | 'ReadMatches' >> fileio.ReadMatches()
                    | 'ParseHTML' >> beam.Map(parse_html)
                    )

    # map outgoing links
    outgoing_links = (parsed_files 
                    | 'CountOutgoingLinks' >> beam.Map(lambda elem: (elem[0], len(elem[1])))
                    )
    
    # map incoming links
    incoming_links = (parsed_files 
                    | 'ExtractLinks' >> beam.FlatMap(lambda elem: elem[1])
                    | 'MapToPage' >> beam.Map(lambda link: (link, 1)) # default count of 1
                    | 'CountIncomingLinks' >> beam.CombinePerKey(sum)
    )

    # return top 5 incoming and outgoing links
    top_outgoing = outgoing_links | 'Top5Outgoing' >> beam.transforms.combiners.Top.Of(5, key=lambda x: x[1])
    top_incoming = incoming_links | 'Top5Incoming' >> beam.transforms.combiners.Top.Of(5, key=lambda x: x[1])

    # write results to bucket
    top_outgoing | 'WriteTop5Outgoing' >> beam.io.WriteToText('gs://ds561-hw2-html-linked-bucket/top_outgoing_links', file_name_suffix='.txt')
    top_incoming | 'WriteTop5Incoming' >> beam.io.WriteToText('gs://ds561-hw2-html-linked-bucket/top_incoming_links', file_name_suffix='.txt')

if __name__ == '__main__':
    pipeline.run()