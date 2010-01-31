#!/bin/sh
# This is a customized version of what dvipdf taken from here
# http://brneurosci.org/linuxsetup73.htm
# It creates higher quality images in the PDF than with 
# dvipdf (maybe dvipdf has arguments do this too).

dvips -Pamz -Pcmz -t letter -o $1.ps $1.dvi
ps2pdf -dMaxSubsetPct=100 -dCompatibilityLevel=1.2  -dSubsetFonts=true \
       -dEmbedAllFonts=true -dAutoFilterColorImages=false \
       -dAutoFilterGrayImages=false -dColorImageFilter=/FlateEncode \
       -dGrayImageFilter=/FlateEncode \
       -dModoImageFilter=/FlateEncode  $1.ps

