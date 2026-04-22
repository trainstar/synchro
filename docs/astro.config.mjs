// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

export default defineConfig({
	site: 'https://trainstar.github.io',
	base: '/synchro',
	integrations: [
		starlight({
			title: 'Synchro',
			disable404Route: true,
			logo: {
				light: './src/assets/logo.svg',
				dark: './src/assets/logo-dark.svg',
				replacesTitle: true,
			},
			favicon: '/favicon.svg',
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/trainstar/synchro' },
			],
			head: [
				{
					tag: 'script',
					attrs: { type: 'module' },
					content: `import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs'; mermaid.initialize({ startOnLoad: true, theme: 'dark' });`,
				},
			],
			sidebar: [
				{
					label: 'Overview',
					items: [
						{ label: 'Welcome', slug: '' },
						{ label: 'Quickstart', slug: 'getting-started/quickstart' },
					],
				},
				{
					label: 'Architecture',
					items: [
						{ label: 'Overview', slug: 'architecture/overview' },
						{ label: 'Scope Modeling', slug: 'architecture/scope-modeling' },
						{ label: 'Portable Seeds', slug: 'architecture/portable-seeds' },
						{ label: 'Auth Integration', slug: 'architecture/auth-integration' },
					],
				},
				{
					label: 'Client SDKs',
					items: [
						{ label: 'Overview', slug: 'clients/overview' },
						{ label: 'Consumption', slug: 'clients/consumption' },
					],
				},
				{
					label: 'Specification',
					items: [
						{ label: 'Principles', slug: 'spec/00-principles' },
						{ label: 'Wire Protocol', slug: 'spec/01-wire-protocol' },
						{ label: 'Client Contract', slug: 'spec/02-client-contract' },
						{ label: 'State Machines', slug: 'spec/03-state-machines' },
						{ label: 'Invariants', slug: 'spec/04-invariants' },
						{ label: 'Schema Evolution', slug: 'spec/05-schema-evolution' },
						{ label: 'Conformance Plan', slug: 'spec/06-conformance-plan' },
					],
				},
			],
		}),
	],
});
