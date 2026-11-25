/**
 The unsaved-changes guard on the Variable Styling page (#802).

 A guarded form is a `form[data-unsaved-note]`: the attribute names the
 "Unsaved" note beside its save button, and that note being shown *is* the
 form being dirty — there is no other flag to fall out of step with it.
 `data-unsaved-message` is the sentence the confirm uses for that form.

 A form turns dirty on any `input` or `change` inside it. Code that changes a
 form without firing either (regenerating stops, fitting the range) says so
 with `window.markStyleDirty()` / `window.markRangeDirty()`.

 Before unsaved work is lost the page asks:
 * leaving the page — the browser's own `beforeunload` prompt;
 * following a link inside a `[data-unsaved-guard]` (the style tabs,
   "+ New style", a style's "Start over" and the chooser's "Start from
   scratch");
 * submitting any form while a *different* guarded form is dirty. Submitting
   a dirty form itself is its save, so it is not asked about; nor is a form
   whose `data-unsaved-discards` names the guarded form it throws away anyway
   (Delete, which asks its own question about the style).
 A clean page navigates and submits without a prompt.

 The functions are defined as the script loads, so the page's own script can
 call them whatever order the DOMContentLoaded listeners run in.
 **/

const unsavedGuardScript = document.currentScript;

function showUnsavedNote(noteId) {
    const note = document.getElementById(noteId);
    if (note) {
        note.hidden = false;
    }
}

window.markStyleDirty = function () {
    showUnsavedNote('vs-unsaved');
};

window.markRangeDirty = function () {
    showUnsavedNote('vs-range-unsaved');
};

document.addEventListener('DOMContentLoaded', function () {
    const forms = [...document.querySelectorAll('form[data-unsaved-note]')];
    if (!forms.length) {
        return;
    }
    const suffix = unsavedGuardScript ? unsavedGuardScript.dataset.confirmSuffix || '' : '';
    // Set once a prompt has been answered for this navigation, so the
    // `beforeunload` that follows does not ask a second time.
    let leaving = false;

    function isUnsaved(form) {
        const note = document.getElementById(form.dataset.unsavedNote);
        return Boolean(note) && !note.hidden;
    }

    function mayDiscard(candidates) {
        const dirty = candidates.filter(isUnsaved);
        if (!dirty.length) {
            return true;
        }
        const lines = dirty.map(function (form) {
            return form.dataset.unsavedMessage;
        });
        return window.confirm(lines.concat(suffix).join(' ').trim());
    }

    forms.forEach(function (form) {
        ['input', 'change'].forEach(function (type) {
            form.addEventListener(type, function () {
                showUnsavedNote(form.dataset.unsavedNote);
            });
        });
    });

    document.querySelectorAll('[data-unsaved-guard]').forEach(function (area) {
        area.addEventListener('click', function (event) {
            const link = event.target.closest('a[href]');
            if (!link || event.defaultPrevented || event.button !== 0
                || event.ctrlKey || event.metaKey || event.shiftKey || event.altKey) {
                return;
            }
            if (mayDiscard(forms)) {
                leaving = true;
            } else {
                event.preventDefault();
            }
        });
    });

    // Bubbling, so a form's own confirm (Delete) has already had its say.
    document.addEventListener('submit', function (event) {
        if (event.defaultPrevented) {
            return;
        }
        const discards = event.target.dataset ? event.target.dataset.unsavedDiscards : undefined;
        const others = forms.filter(function (form) {
            return form !== event.target && form.id !== discards;
        });
        if (mayDiscard(others)) {
            leaving = true;
        } else {
            event.preventDefault();
        }
    });

    window.addEventListener('beforeunload', function (event) {
        if (!leaving && forms.some(isUnsaved)) {
            event.preventDefault();
            event.returnValue = '';
        }
    });

    // Back to this page from the history cache: whatever was leaving did not.
    window.addEventListener('pageshow', function () {
        leaving = false;
    });
});
