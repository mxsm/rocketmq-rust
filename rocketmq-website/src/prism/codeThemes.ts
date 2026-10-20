import type {PrismTheme} from 'prism-react-renderer';

/*
 * Syntax palettes for code blocks. `docusaurus.config.ts` uses them for Markdown
 * code fences in each colour mode; the homepage showcase reuses the dark one so
 * code reads the same everywhere on the site.
 */

/** For dark surfaces. Token colours keep at least 4.5:1 against the background. */
export const darkCodeTheme: PrismTheme = {
  plain: {color: '#c9d1e4', backgroundColor: '#0d1220'},
  styles: [
    {types: ['comment', 'prolog', 'doctype', 'cdata'], style: {color: '#7f8aa3', fontStyle: 'italic'}},
    {types: ['punctuation', 'operator'], style: {color: '#8f99b2'}},
    {types: ['keyword', 'boolean', 'important'], style: {color: '#c792ea'}},
    {types: ['string', 'char', 'attr-value', 'inserted'], style: {color: '#c3e88d'}},
    {types: ['number', 'constant', 'symbol'], style: {color: '#f78c6c'}},
    {types: ['function', 'function-definition', 'builtin'], style: {color: '#82aaff'}},
    {types: ['property', 'key', 'table', 'tag', 'atrule'], style: {color: '#82aaff'}},
    {types: ['macro', 'attribute', 'attr-name'], style: {color: '#ffcb6b'}},
    {types: ['class-name', 'type-definition', 'namespace'], style: {color: '#ffb86b'}},
    {types: ['variable', 'parameter'], style: {color: '#89ddff'}},
    {types: ['deleted'], style: {color: '#ff8fa3'}},
  ],
};

/** For light surfaces. Token colours keep at least 4.5:1 against the background. */
export const lightCodeTheme: PrismTheme = {
  plain: {color: '#2b3040', backgroundColor: '#f6f7fb'},
  styles: [
    {types: ['comment', 'prolog', 'doctype', 'cdata'], style: {color: '#687182', fontStyle: 'italic'}},
    {types: ['punctuation', 'operator'], style: {color: '#5a6375'}},
    {types: ['keyword', 'boolean', 'important'], style: {color: '#7c3aed'}},
    {types: ['string', 'char', 'attr-value', 'inserted'], style: {color: '#0b7a4b'}},
    {types: ['number', 'constant', 'symbol'], style: {color: '#b4470c'}},
    {types: ['function', 'function-definition', 'builtin'], style: {color: '#1f55c9'}},
    {types: ['property', 'key', 'table', 'tag', 'atrule'], style: {color: '#1f55c9'}},
    {types: ['macro', 'attribute', 'attr-name'], style: {color: '#946000'}},
    {types: ['class-name', 'type-definition', 'namespace'], style: {color: '#a8501a'}},
    {types: ['variable', 'parameter'], style: {color: '#0e7490'}},
    {types: ['deleted'], style: {color: '#c62846'}},
  ],
};
