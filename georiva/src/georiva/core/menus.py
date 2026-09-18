"""The admin menus core owns and plugins register into.

A registration module is imported for its side effects and is nobody's import
target, so the seam a plugin reaches for lives here — the way ``storage`` and the
registries are imported from their own modules rather than from wherever they
happen to be wired up.
"""

from wagtail.admin.menu import Menu

#: The hook a publisher plugin registers its menu item through. Core owns the
#: group and knows none of its children — that is what makes them plugins.
PUBLICATIONS_MENU_HOOK = "register_publications_menu_item"

#: One menu for the life of the process, as Wagtail's own ``admin_menu`` and
#: ``settings_menu`` are: the hook's answers are cached on the instance, so the
#: instance has to be the same one every request sees.
publications_menu = Menu(register_hook_name=PUBLICATIONS_MENU_HOOK)
