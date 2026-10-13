module.exports = {
  // set by the Pages workflow, e.g. "/picard" for project sites
  pathPrefix: process.env.PATH_PREFIX || '',
  siteMetadata: {
    title: 'Picard | Rust Actors',
  },
  plugins: ['gatsby-plugin-react-helmet', 'gatsby-plugin-styled-components'],
}
