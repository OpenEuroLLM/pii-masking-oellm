import sys
import orjson
import argparse
import datetime
import contextlib
from pathlib import Path
from loguru import logger
from pii_manager import PiiEnum
from pii_manager.api import PiiManager
from pii_manager.lang import COUNTRY_ANY


buffer_option = orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_APPEND_NEWLINE

@contextlib.contextmanager
def stdout_to_err():
    save_stdout = sys.stdout
    sys.stdout = sys.stderr
    yield
    sys.stdout = save_stdout

def get_id_field(doc, id_field, metadata):
    return doc[metadata][id_field] if metadata else doc[id_field]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--lang", type = str, required = True, 
                        help = "ISO 639-1 2-char language code. E.g.: 'en', 'es'")
    parser.add_argument("--id-field", type = str, required = True,
                        help = "Name of ID field for the dataset being processed.")
    parser.add_argument("--metadata-field", type = str, default = "", 
                        help = "Specific metadata field where document \
                        IDs are. E.g. DCLM has 'metadata'.")
    parser.add_argument("--pii-mode", type = str, 
                        required = True, default = "extract",
                        choices = ["full", "extract", "replace"],
                        help = "Tool's mode of PII processing. Allowed modes: \
                            'full', 'extract', 'replace'")
    args = parser.parse_args()

    # Create logs folder if it doesn't exist already
    Path("logs").mkdir(exist_ok=True, parents=True)
    logger.add(f"logs/pii_{args.pii_mode}_{datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}.log")

    # All tasks used for this PII iteration
    tasklist = (PiiEnum.CREDIT_CARD,
                PiiEnum.GOV_ID,
                PiiEnum.BITCOIN_ADDRESS,
                PiiEnum.IP_ADDRESS,
                PiiEnum.EMAIL_ADDRESS,
                PiiEnum.PHONE_NUMBER,
                PiiEnum.BANK_ACCOUNT,
                PiiEnum.LICENSE_PLATE,
                PiiEnum.DRIVER_LICENSE)

    # Initialize PII tool with context set for optimized JSONL stdout output
    with stdout_to_err():
        proc = PiiManager(args.lang, COUNTRY_ANY,
                          tasks = tasklist,
                          mode = args.pii_mode)

    try:
        for line in sys.stdin:
            doc = orjson.loads(line)
            
            # Call PII tool
            result = proc(doc["text"])

            if args.pii_mode == "replace":
                if result != doc["text"]:
                    result_doc = {}
                    result_doc[args.id_field] = get_id_field(doc, 
                                                            args.id_field, 
                                                            args.metadata_field)
                    result_doc["text"] = result

                    sys.stdout.buffer.write(orjson.dumps(result_doc, 
                                                        option = buffer_option))
            
            elif args.pii_mode == "full":
                if result["text"] != doc["text"]:
                    result_doc = {}
                    result_doc[args.id_field] = get_id_field(doc, 
                                                            args.id_field, 
                                                            args.metadata_field)
                    result_doc["text"] = result["text"]
                    result_doc["entities"] = result["entities"]

                    sys.stdout.buffer.write(orjson.dumps(result_doc, 
                                                        option = buffer_option))

            elif args.pii_mode == "extract":
                # Result here becomes a generator, so just extract everything
                # into a list for easier processing 
                result_list = list(result)
                if result_list:
                    for pii in result_list:
                        result_doc = {}
                        result_doc[args.id_field] = get_id_field(doc, 
                                                                args.id_field, 
                                                                args.metadata_field)
                        result_doc["name"] = pii.elem.name
                        result_doc["value"] = pii.value
                        result_doc["start_pos"] = pii.pos
                        result_doc["end_pos"] = pii.pos + len(pii.value)

                        sys.stdout.buffer.write(orjson.dumps(result_doc,
                                                            option = buffer_option))

    except Exception as e:
        logger.error(f"PII job failed: {e}")
        raise RuntimeError(f"PII job failed: {e}") from e

if __name__ == "__main__":
    main()
