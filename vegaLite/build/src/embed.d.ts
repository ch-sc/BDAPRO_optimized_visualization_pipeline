import * as vegaImport from 'vega-lib';
import { EncodeEntryName, Loader, LoaderOptions, Renderers, Spec as VgSpec, TooltipHandler, View } from 'vega-lib';
import * as vlImport from 'vega-lite';
import { TopLevelSpec as VlSpec } from 'vega-lite';
import { Options as TooltipOptions } from 'vega-tooltip';
import { Config, Mode } from './types';
import { DeepPartial } from './util';
export * from './types';
export declare const vega: typeof vegaImport;
export declare const vl: typeof vlImport;
export interface Actions {
    export?: boolean | {
        svg?: boolean;
        png?: boolean;
    };
    source?: boolean;
    compiled?: boolean;
    editor?: boolean;
}
export interface Hover {
    hoverSet?: EncodeEntryName;
    updateSet?: EncodeEntryName;
}
export declare type PatchFunc = (spec: VgSpec) => VgSpec;
export interface EmbedOptions {
    actions?: boolean | Actions;
    mode?: Mode;
    theme?: 'excel' | 'ggplot2' | 'quartz' | 'vox' | 'dark';
    defaultStyle?: boolean | string;
    logLevel?: number;
    loader?: Loader | LoaderOptions;
    renderer?: Renderers;
    tooltip?: TooltipHandler | TooltipOptions | boolean;
    patch?: PatchFunc | DeepPartial<VgSpec>;
    onBeforeParse?: PatchFunc;
    width?: number;
    height?: number;
    padding?: number | {
        left?: number;
        right?: number;
        top?: number;
        bottom?: number;
    };
    scaleFactor?: number;
    config?: string | Config;
    sourceHeader?: string;
    sourceFooter?: string;
    editorUrl?: string;
    hover?: boolean | Hover;
    runAsync?: boolean;
    i18n?: Partial<typeof I18N>;
}
declare const I18N: {
    CLICK_TO_VIEW_ACTIONS: string;
    COMPILED_ACTION: string;
    EDITOR_ACTION: string;
    PNG_ACTION: string;
    SOURCE_ACTION: string;
    SVG_ACTION: string;
};
export declare type VisualizationSpec = VlSpec | VgSpec;
export interface Result {
    /** The Vega view. */
    view: View;
    /** The inut specification. */
    spec: VisualizationSpec;
    /** The compiled and patched Vega specification. */
    vgSpec: VgSpec;
}
/**
 * Try to guess the type of spec.
 *
 * @param spec Vega or Vega-Lite spec.
 */
export declare function guessMode(spec: VisualizationSpec, providedMode?: Mode): Mode;
/**
 * Embed a Vega visualization component in a web page. This function returns a promise.
 *
 * @param el        DOM element in which to place component (DOM node or CSS selector).
 * @param spec      String : A URL string from which to load the Vega specification.
 *                  Object : The Vega/Vega-Lite specification as a parsed JSON object.
 * @param opt       A JavaScript object containing options for embedding.
 */
export default function embed(el: HTMLElement | string, spec: VisualizationSpec | string, opt?: EmbedOptions): Promise<Result>;
