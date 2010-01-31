.SUFFIXES: .gnuplot .tex .eps .dat

default: test.tex

.gnuplot.tex: $<
	gnuplot $<

.gnuplot.png: $<
	gnuplot $<

clean:
	rm -rf *~ *.tex *.eps

test.tex: test.dat test.gnuplot
