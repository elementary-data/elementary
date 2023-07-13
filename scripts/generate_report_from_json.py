import pathlib
import sys


def gen_report(json_output_path: str):
    json_output_data = pathlib.Path(json_output_path).read_text()
    report_template_path = (
        pathlib.Path(__file__).parent.parent
        / "elementary"
        / "monitor"
        / "data_monitoring"
        / "report"
        / "index.html"
    )
    report_template_data = report_template_path.read_text()
    compiled_output_html = f"{report_template_data}<script>var elementaryData = {json_output_data}</script>"
    with open("manually_generated_report.html", "w", encoding="utf-8") as report_file:
        report_file.write(compiled_output_html)


def main():
    if len(sys.argv) < 2:
        raise ValueError("Please provide the JSON output file path as an argument.")
    json_output_path = sys.argv[1]
    gen_report(json_output_path)


if __name__ == "__main__":
    main()
