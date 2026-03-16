// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

export default defineConfig({
	site: 'https://trainstar.github.io',
	base: '/synchro',
	integrations: [
		starlight({
			title: 'Synchro',
			logo: {
				src: './src/assets/logo.svg',
				replacesTitle: true,
			},
			favicon: '/favicon.svg',
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/trainstar/synchro' },
			],
			editLink: {
				baseUrl: 'https://github.com/trainstar/synchro/edit/master/docs/src/content/docs/',
			},
			sidebar: [
				{
					label: 'Getting Started',
					items: [
						{ label: 'Quick Start', slug: 'getting-started/quickstart' },
						{ label: 'Core Concepts', slug: 'getting-started/concepts' },
					],
				},
				{
					label: 'Server',
					items: [
						{ label: 'Configuration', slug: 'server/configuration' },
						{ label: 'Architecture', slug: 'server/architecture' },
						{ label: 'Deployment', slug: 'server/deployment' },
						{ label: 'Type Reference', slug: 'server/types' },
					],
				},
				{
					label: 'Client SDKs',
					items: [
						{ label: 'Overview', slug: 'clients/overview' },
						{ label: 'Swift / iOS', slug: 'clients/swift' },
						{ label: 'Kotlin / Android', slug: 'clients/kotlin' },
						{ label: 'React Native', slug: 'clients/react-native' },
					],
				},
				{
					label: 'Protocol',
					items: [
						{ label: 'API Reference', slug: 'protocol/api-reference' },
					],
				},
			],
		}),
	],
});
