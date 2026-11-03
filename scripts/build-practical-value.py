"""Сборка одностраничного PDF из docs/practical-value.md через ReportLab."""
import os
from pathlib import Path
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import SimpleDocTemplate, Paragraph


ROOT = Path(__file__).resolve().parents[1]
font_candidates = [
    Path(os.environ.get("REPORT_FONT", "__not_set__")),
    Path("C:/Windows/Fonts/arial.ttf"),
    Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
]
font = next((p for p in font_candidates if p.is_file()), None)
if font is None:
    raise FileNotFoundError("Задайте REPORT_FONT: путь к Unicode TTF со шрифтом кириллицы.")
pdfmetrics.registerFont(TTFont("ReportFont", str(font)))
styles = {
    "title": ParagraphStyle("title", fontName="ReportFont", fontSize=18, leading=22,
                            textColor=colors.HexColor("#14374A"), spaceAfter=7),
    "subtitle": ParagraphStyle("subtitle", fontName="ReportFont", fontSize=9, leading=12,
                               textColor=colors.HexColor("#52636D"), spaceAfter=13),
    "heading": ParagraphStyle("heading", fontName="ReportFont", fontSize=10.6, leading=14,
                              textColor=colors.HexColor("#14374A"), spaceAfter=4),
    "body": ParagraphStyle("body", fontName="ReportFont", fontSize=10, leading=13.2,
                           alignment=TA_LEFT, spaceAfter=9),
}
blocks = (ROOT / "docs/practical-value.md").read_text(encoding="utf-8").strip().split("\n\n")
story = []
for i, block in enumerate(blocks):
    if block.startswith("# "):
        story.append(Paragraph(escape(block[2:]), styles["title"]))
    elif block.startswith("## "):
        story.append(Paragraph(escape(block[3:]), styles["heading"]))
    else:
        story.append(Paragraph(escape(block.replace("\n", " ")),
                               styles["subtitle"] if i == 1 else styles["body"]))
output = ROOT / "docs/practical-value.pdf"
doc = SimpleDocTemplate(str(output), pagesize=A4, leftMargin=43, rightMargin=43,
                        topMargin=36, bottomMargin=34,
                        title="Reels Router: практическая ценность", author="Команда проекта 205")
doc.build(story)
print(output)
