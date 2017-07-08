

from streamcorpus import make_stream_item, Selector, Chunk, Offset, OffsetType
from memex_dossier.streamcorpus_structured.transform import SelectorType

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('out_path')
    args = parser.parse_args()

    si = make_stream_item(1, 'http://crazydog.com')
    si.body.raw = '''
Flying dogs are amazing.
The flight of the super dog Sam Vroomvroom is often cited as the first such flying dog.
'''

    topic_name = 'The flight of the super dog Sam Vroomvroom'
    sel = Selector(
        selector_type=SelectorType.TOPIC.value,
        raw_selector=topic_name,
        canonical_selector=topic_name.lower(), # this is the key for making it appear for a profile of this title
        offsets={OffsetType.CHARS: Offset(
            type=OffsetType.CHARS,
            first=si.body.raw.find('The'),
            length=len(topic_name),
        )},
    )
    si.body.selectors['other'] = [sel]

    chunk = Chunk(args.out_path, mode='wb')
    chunk.add(si)
    chunk.close()


if __name__ == '__main__':
    main()
