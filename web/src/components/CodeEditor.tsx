// CodeMirror 6 wrapper. Loaded only by editor routes (lazy chunks). Syntax
// highlighting is not Sparrow semantic validation; the server decides.
import { useEffect, useRef } from "react";
import { EditorState, Compartment } from "@codemirror/state";
import { EditorView, keymap, lineNumbers, highlightActiveLine, highlightActiveLineGutter, drawSelection, placeholder as ph } from "@codemirror/view";
import { defaultKeymap, history, historyKeymap, indentWithTab } from "@codemirror/commands";
import { bracketMatching, indentOnInput, syntaxHighlighting, HighlightStyle, foldGutter } from "@codemirror/language";
import { searchKeymap, highlightSelectionMatches } from "@codemirror/search";
import { sql } from "@codemirror/lang-sql";
import { json } from "@codemirror/lang-json";
import { tags as t } from "@lezer/highlight";

const hl = HighlightStyle.define([
  { tag: [t.keyword, t.operatorKeyword], color: "var(--cm-kw)", fontWeight: "600" },
  { tag: [t.string, t.special(t.string)], color: "var(--cm-str)" },
  { tag: [t.number, t.bool, t.null], color: "var(--cm-num)" },
  { tag: [t.propertyName, t.attributeName], color: "var(--cm-prop)" },
  { tag: [t.comment, t.lineComment, t.blockComment], color: "var(--text-3)", fontStyle: "italic" },
  { tag: [t.function(t.variableName), t.typeName], color: "var(--cm-fn)" },
  { tag: [t.punctuation, t.bracket], color: "var(--text-2)" },
]);

const theme = EditorView.theme({
  "&": { height: "100%", fontSize: "13px", background: "var(--bg-elev)", color: "var(--text)" },
  ".cm-scroller": { fontFamily: "var(--font-mono)", lineHeight: "1.6" },
  ".cm-gutters": { background: "var(--bg-sunken)", color: "var(--text-3)", border: "none", borderRight: "1px solid var(--border)" },
  ".cm-activeLine": { background: "color-mix(in srgb, var(--accent) 6%, transparent)" },
  ".cm-activeLineGutter": { background: "color-mix(in srgb, var(--accent) 10%, transparent)", color: "var(--text-2)" },
  ".cm-cursor": { borderLeftColor: "var(--accent)" },
  "&.cm-focused .cm-selectionBackground, .cm-selectionBackground, ::selection": { background: "color-mix(in srgb, var(--accent) 22%, transparent) !important" },
  "&.cm-focused": { outline: "none" },
  ".cm-placeholder": { color: "var(--text-3)" },
  ".cm-panels": { background: "var(--bg-sunken)", color: "var(--text)" },
});

export interface CodeEditorProps {
  value: string;
  onChange: (v: string) => void;
  language: "sql" | "json";
  label: string;
  readOnly?: boolean;
  placeholder?: string;
  onSave?: () => void;
  minHeight?: number;
}

export function CodeEditor({ value, onChange, language, label, readOnly = false, placeholder, onSave, minHeight = 240 }: CodeEditorProps) {
  const host = useRef<HTMLDivElement>(null);
  const view = useRef<EditorView | null>(null);
  const cb = useRef({ onChange, onSave });
  cb.current = { onChange, onSave };
  const ro = useRef(new Compartment());

  useEffect(() => {
    const state = EditorState.create({
      doc: value,
      extensions: [
        lineNumbers(), highlightActiveLineGutter(), foldGutter(), history(), drawSelection(), indentOnInput(),
        bracketMatching(), highlightActiveLine(), highlightSelectionMatches(),
        syntaxHighlighting(hl), theme, language === "sql" ? sql() : json(),
        ph(placeholder ?? ""),
        ro.current.of([EditorState.readOnly.of(readOnly), EditorView.editable.of(!readOnly)]),
        keymap.of([
          { key: "Mod-s", preventDefault: true, run: () => { cb.current.onSave?.(); return true; } },
          indentWithTab, ...defaultKeymap, ...historyKeymap, ...searchKeymap,
        ]),
        EditorView.updateListener.of((u) => { if (u.docChanged) cb.current.onChange(u.state.doc.toString()); }),
        EditorView.contentAttributes.of({ "aria-label": label }),
      ],
    });
    view.current = new EditorView({ state, parent: host.current! });
    return () => { view.current?.destroy(); view.current = null; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [language]);

  // External value changes (reload/merge) replace the document.
  useEffect(() => {
    const v = view.current;
    if (v && v.state.doc.toString() !== value) v.dispatch({ changes: { from: 0, to: v.state.doc.length, insert: value } });
  }, [value]);
  useEffect(() => {
    view.current?.dispatch({ effects: ro.current.reconfigure([EditorState.readOnly.of(readOnly), EditorView.editable.of(!readOnly)]) });
  }, [readOnly]);

  return <div className="code-editor" ref={host} style={{ minHeight }} data-testid={`editor-${language}`} />;
}
