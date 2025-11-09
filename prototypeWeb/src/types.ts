export enum ArticleStatus {
  PUBLISHED = 'published',
  DRAFT = 'draft',
  ARCHIVED = 'archived'
}

export interface Article {
  id: string;
  name: string;
  status: ArticleStatus;
}
