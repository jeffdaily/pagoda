.SUFFIXES: .gnuplot .tex .eps .dat

default: strong.tex strong_io.tex

.gnuplot.tex: $<
	gnuplot $<

.gnuplot.png: $<
	gnuplot $<

clean:
	rm -rf *~ *.tex *.eps *.png

strong.tex: strong.dat strong.gnuplot
