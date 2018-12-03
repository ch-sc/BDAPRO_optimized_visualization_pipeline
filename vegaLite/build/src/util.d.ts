/**
 * From Vega-Lite
 */
export declare type DeepPartial<T> = {
    [P in keyof T]?: DeepPartial<T[P]>;
};
export declare function mergeDeep<T>(dest: T, ...src: Array<DeepPartial<T>>): T;
export declare function isURL(s: string): boolean;
