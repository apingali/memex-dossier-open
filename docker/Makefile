
GITDESCRIBE := $(shell git describe | cut -d- -f-2)
DIST := memex-dossier-$(GITDESCRIBE)-$(shell date -u +%Y-%m-%d)
BUILD := ../../memex-dossier-build

clean:
	rm -rf $(BUILD) $(DIST)

$(BUILD):
	echo $(DIST)
	mkdir -p $@

container: ../../memex-dossier-build
	cd ../../memex-dossier-build && ../memex-dossier/docker/setup.sh && docker build . --tag memex-dossier:latest


$(DIST)/memex-dossier-docker-image.tar:
	mkdir -p $(DIST)
	docker save memex-dossier:latest > $@

$(DIST)/memex-dossier-source-code.tar:
	cd .. && git archive --format=tar --prefix=memex-dossier-$(GITDESCRIBE)/ master > /tmp/mdsc.tar
	mv /tmp/mdsc.tar $@

$(DIST)/web-navigator-source-code.tar:
	cd ../../web-navigator && git archive --format=tar --prefix=memex-dossier-$(GITDESCRIBE)/ master > /tmp/wnsc.tar
	mv /tmp/wnsc.tar $@

release: container \
		$(DIST)/memex-dossier-docker-image.tar \
		$(DIST)/memex-dossier-source-code.tar \
		$(DIST)/web-navigator-source-code.tar
	cp README.md $(DIST)
	cp ../configs/config-example.yaml $(DIST)/config.yaml
	cp docker-compose.yml $(DIST)
	cp `ls ../../web-navigator/releases/diffeo-memex-web-navigator-*.crx | tail -1` $(DIST)
	tar czf $(DIST).tar.gz $(DIST)
	s3cmd put $(DIST).tar.gz s3://diffeo-memex/releases/ --acl-public
