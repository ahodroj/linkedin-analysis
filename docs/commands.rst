Commands
========

The Makefile contains the central entry points for common tasks related to this project.

Syncing data to your remote drive
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* `make copy_data` will use `rclone copyto` to recursively sync files in `data/` up to `google:/Data/linkedin`.
* `make sync_data` will use `rclone sync` to recursively sync files from `google:/Data/linkedin` to `data/`.
