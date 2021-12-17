RELEASE_VERSION = $(realease_version)

ifeq ("$(RELEASE_VERSION)" , "")
	RELEASE_VERSION := "dev"
endif

.PHONY: print
print:; $( info ==== xixi)
	@echo "123"
