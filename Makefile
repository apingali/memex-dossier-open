

RELNAME="memex-dossier-`python version.py --string`"

release:
	git archive --format tar.gz -o $(RELNAME).tar.gz  --prefix $(RELNAME)/ HEAD
	md5sum $(RELNAME).tar.gz > tmp.md5
	mv tmp.md5 $(RELNAME).tar.gz."`cat tmp.md5 | cut -c -32`".md5
	s3cmd put $(RELNAME).tar.gz* s3://diffeo-memex/releases/
