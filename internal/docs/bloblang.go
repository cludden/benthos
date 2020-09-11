package docs

import (
	"bytes"
	"strings"
	"text/template"

	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
)

type functionCategory struct {
	Name  string
	Specs []query.FunctionSpec
}

type functionsContext struct {
	Categories []functionCategory
}

var bloblangFunctionsTemplate = `{{define "function_example" -}}
{{if gt (len .Summary) 0 -}}
{{.Summary}}

{{end -}}

` + "```coffee" + `
{{.Mapping}}
{{range $i, $result := .Results}}
# In:  {{index $result 0}}
# Out: {{index $result 1}}
{{end -}}
` + "```" + `
{{end -}}

{{define "function_spec" -}}
### ` + "`{{.Name}}`" + `

{{if .Beta -}}
BETA: This function is experimental and therefore subject to change outside of major version releases.

{{end -}}
{{.Description}}
{{range $i, $example := .Examples}}
{{template "function_example" $example -}}
{{end -}}

{{end -}}

---
title: Bloblang Functions
sidebar_label: Functions
description: A list of Bloblang functions
---

<!--
     THIS FILE IS AUTOGENERATED!

     To make changes please edit the contents of:
     internal/bloblang/query/functions.go
     internal/docs/bloblang.go
-->

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Functions can be placed anywhere and allow you to extract information from your environment, generate values, or access data from the underlying message being mapped:

` + "```coffee" + `
root.doc.id = uuid_v4()
root.doc.received_at = timestamp_unix()
root.doc.host = hostname()
` + "```" + `

{{range $i, $cat := .Categories -}}
## {{$cat.Name}}

{{range $i, $spec := $cat.Specs -}}
{{template "function_spec" $spec}}
{{end -}}
{{end -}}

[error_handling]: /docs/configuration/error_handling
[field_paths]: /docs/configuration/field_paths
[meta_proc]: /docs/components/processors/metadata
[methods.encode]: /docs/guides/bloblang/methods#encode
[methods.string]: /docs/guides/bloblang/methods#string
`

// BloblangFunctionsMarkdown returns a markdown document for all Bloblang
// functions.
func BloblangFunctionsMarkdown() ([]byte, error) {
	ctx := functionsContext{}

	specs := query.FunctionDocs()

	for _, cat := range []query.FunctionCategory{
		query.FunctionCategoryGeneral,
		query.FunctionCategoryMessage,
		query.FunctionCategoryEnvironment,
	} {
		functions := functionCategory{
			Name: string(cat),
		}
		for _, spec := range specs {
			if spec.Category == cat {
				functions.Specs = append(functions.Specs, spec)
			}
		}
		ctx.Categories = append(ctx.Categories, functions)
	}

	var buf bytes.Buffer
	err := template.Must(template.New("functions").Parse(bloblangFunctionsTemplate)).Execute(&buf, ctx)

	return buf.Bytes(), err
}

//------------------------------------------------------------------------------

type methodCategory struct {
	Name  string
	Specs []query.MethodSpec
}

type methodsContext struct {
	Categories []methodCategory
	General    []query.MethodSpec
}

var bloblangMethodsTemplate = `{{define "method_example" -}}
{{if gt (len .Summary) 0 -}}
{{.Summary}}

{{end -}}

` + "```coffee" + `
{{.Mapping}}
{{range $i, $result := .Results}}
# In:  {{index $result 0}}
# Out: {{index $result 1}}
{{end -}}
` + "```" + `
{{end -}}

{{define "method_spec" -}}
### ` + "`{{.Name}}`" + `

{{if .Beta -}}
BETA: This method is experimental and therefore subject to change outside of major version releases.

{{end -}}
{{.Description}}
{{range $i, $example := .Examples}}
{{template "method_example" $example -}}
{{end -}}

{{end -}}

---
title: Bloblang Methods
sidebar_label: Methods
description: A list of Bloblang methods
---

<!--
     THIS FILE IS AUTOGENERATED!

     To make changes please edit the contents of:
     internal/bloblang/query/methods.go
     internal/bloblang/query/methods_strings.go
     internal/docs/bloblang.go
-->

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Methods provide most of the power in Bloblang as they allow you to augment values and can be added to any expression (including other methods):

` + "```coffee" + `
root.doc.id = this.thing.id.string().catch(uuid_v4())
root.doc.reduced_nums = this.thing.nums.for_each(
  match this {
    this < 10 => deleted()
    _ => this - 10
  }
)
root.has_good_taste = ["pikachu","mewtwo","magmar"].contains(
  this.user.fav_pokemon
)
` + "```" + `

{{if gt (len .General) 0 -}}
## General

{{range $i, $spec := .General -}}
{{template "method_spec" $spec}}
{{end -}}
{{end -}}

{{range $i, $cat := .Categories -}}
## {{$cat.Name}}

{{range $i, $spec := $cat.Specs -}}
{{template "method_spec" $spec}}
{{end -}}
{{end -}}

[field_paths]: /docs/configuration/field_paths
[methods.encode]: #encode
[methods.string]: #string
`

func methodForCat(s query.MethodSpec, cat query.MethodCategory) (query.MethodSpec, bool) {
	for _, c := range s.Categories {
		if c.Category == cat {
			spec := s
			if len(c.Description) > 0 {
				spec.Description = strings.TrimSpace(c.Description)
			}
			if len(c.Examples) > 0 {
				spec.Examples = c.Examples
			}
			return spec, true
		}
	}
	return s, false
}

// BloblangMethodsMarkdown returns a markdown document for all Bloblang methods.
func BloblangMethodsMarkdown() ([]byte, error) {
	ctx := methodsContext{}

	specs := query.MethodDocs()

	for _, cat := range []query.MethodCategory{
		query.MethodCategoryStrings,
		query.MethodCategoryRegexp,
		query.MethodCategoryCoercion,
		query.MethodCategoryObjectAndArray,
		query.MethodCategoryParsing,
		query.MethodCategoryEncoding,
	} {
		methods := methodCategory{
			Name: string(cat),
		}
		for _, spec := range specs {
			var ok bool
			if spec, ok = methodForCat(spec, cat); ok {
				methods.Specs = append(methods.Specs, spec)
			}
		}
		ctx.Categories = append(ctx.Categories, methods)
	}

	for _, spec := range specs {
		if len(spec.Categories) == 0 && !spec.Deprecated {
			spec.Description = strings.TrimSpace(spec.Description)
			ctx.General = append(ctx.General, spec)
		}
	}

	var buf bytes.Buffer
	err := template.Must(template.New("methods").Parse(bloblangMethodsTemplate)).Execute(&buf, ctx)

	return buf.Bytes(), err
}