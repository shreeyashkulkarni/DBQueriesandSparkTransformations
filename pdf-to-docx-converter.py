from pdf2docx import Converter

pdf_file = 'C:\\Users\\shree\\Downloads\\newcapgem.pdf'
docx_file = 'C:\\Users\\shree\\Downloads\\newcapgem.docx'

cv = Converter(pdf_file)
cv.convert(docx_file)
cv.close()