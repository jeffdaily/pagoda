TARGET = paper
LATEX = latex
DVIPDF = ./mydvipdf.sh

paper:
	#cd plots && $(MAKE)
	$(LATEX) $@ && \
	bibtex $@ && \
	$(LATEX) $@ && \
	$(LATEX) $@ && \
	$(LATEX) $@ && \
	$(DVIPDF) $@ && \
	wc -w *.tex | tail -n 1

clean:
	rm -rf *~ *.log *.lof *.lot *.aux *.bbl *.blg *.dvi *.eps $(TARGET).toc $(TARGET).ps $(TARGET).pdf
	cd plots && $(MAKE) clean

$(TARGET): *.tex
