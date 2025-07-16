include .env
export

# =-=-=--=-=-=-=-=-=-=-=--=-=-=-=-=-
#   	 Environment Variables
# =-=-=--=-=-=-=-=-=-=-=--=-=-=-=-=-

# Check for environment and set activation command
ifdef VIRTUAL_ENV
    # Already in a virtual environment
    ACTIVATE = @echo "venv - $(VIRTUAL_ENV)" &&
    PYTHON = python
else ifdef CONDA_DEFAULT_ENV
    # Already in conda environment
    ACTIVATE = @echo "conda - $(CONDA_DEFAULT_ENV)" &&
    PYTHON = python
else ifeq ($(wildcard venv/Scripts/activate),venv/Scripts/activate)
    # Windows venv available
    ACTIVATE = @venv\Scripts\activate &&
    PYTHON = python
else ifeq ($(wildcard venv/bin/activate),venv/bin/activate)
    # Unix venv available
    ACTIVATE = @source venv/bin/activate &&
    PYTHON = python3
else
    # No environment found
    ACTIVATE = @echo "❌ No environment found. Run 'make venv' or activate conda." && exit 1 &&
    PYTHON = python
endif

.PHONY: all venv env-status install-init install install-update install-requirements \

# =-=-=--=-=-=-=-=-=-=-=--=-=-=-=-=-
#  			Python Environment
# =-=-=--=-=-=-=-=-=-=-=--=-=-=-=-=-
venv:
	@$(PYTHON) -m venv venv
	@echo "✅ Virtual environment created. Activate with:"
	@echo "   source venv/bin/activate  (macOS/Linux)"
	@echo "   venv\\Scripts\\activate     (Windows)"

env-status:
	@echo "=== Environment Status ==="
	$(ACTIVATE) echo "Python: $$(which $(PYTHON))"

# =-=-=--=-=-=-=-=-=-=
# Package Installation
# =-=-=--=-=-=-=-=-=-=
install-init:
	$(ACTIVATE) $(PYTHON) -m pip install pip-tools
	$(ACTIVATE) $(PYTHON) -m piptools compile requirements.in
	$(ACTIVATE) $(PYTHON) -m piptools sync requirements.txt

install:
	grep "^${pkg}" requirements.in || (echo "" >> requirements.in && echo "${pkg}" >> requirements.in)
	$(ACTIVATE) $(PYTHON) -m piptools compile requirements.in
	$(ACTIVATE) $(PYTHON) -m piptools sync requirements.txt

install-update:
	$(ACTIVATE) $(PYTHON) -m piptools compile requirements.in
	$(ACTIVATE) $(PYTHON) -m piptools sync requirements.txt

install-requirements:
	$(ACTIVATE) $(PYTHON) -m piptools sync requirements.txt


# =-=-=--=-=-=-=-=-=-=-=--=-=-=-=-=-
#  		Change .env Commands
# =-=-=--=-=-=-=-=-=-=-=--=-=-=-=-=-
use-remote-db:
	cp .env.remote .env
	@echo "Switched to REMOTE database"

use-local-db:
	cp .env.local .env
	@echo "Switched to LOCAL database"
