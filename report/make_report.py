import markdown2
import weasyprint

with open("report/comparison_report.md", "r") as f:
    markdown_text = f.read()

html = markdown2.markdown(markdown_text, extras=["fenced-code-blocks"])
weasyprint.HTML(string=html).write_pdf("report/comparison_report.pdf")
