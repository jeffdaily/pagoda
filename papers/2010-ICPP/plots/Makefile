.SUFFIXES: .gnuplot .tex .eps .dat

default: strong.tex strong_hist.tex strong_io.tex strong_io_hist.tex

.gnuplot.tex: $<
	gnuplot $<

.gnuplot.png: $<
	gnuplot $<

clean:
	rm -rf *~ *.tex *.eps *.png

strong.tex: strong.dat strong.gnuplot
strong_hist.tex: strong.dat strong_hist.gnuplot
strong_io.tex: strong.dat strong_io.gnuplot
strong_io_hist.tex: strong.dat strong_io_hist.gnuplot
