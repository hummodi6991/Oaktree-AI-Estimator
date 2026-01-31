export type SearchItem = {
  type: string;
  id: string;
  label: string;
  subtitle?: string | null;
  center: [number, number];
  bbox?: [number, number, number, number] | null;
};

export type SearchResponse = {
  items: SearchItem[];
};
