paper.pdf: paper.tex paper.bbl
	pdflatex paper.tex

paper.bbl: paper.bib paper.aux
	bibtex paper.aux

paper.aux: paper.tex
	latex paper.tex

clean:
	rm -f paper.pdf paper.log paper.aux paper.dvi paper.blg paper.bbl
