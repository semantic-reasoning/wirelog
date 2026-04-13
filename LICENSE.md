# wirelog License

wirelog is available under dual licensing:

## 1. Open Source License: LGPL-3.0

wirelog is distributed under the **GNU Lesser General Public License v3.0 (LGPL-3.0)**.

This license allows you to:
- Use wirelog as a library in your applications
- Modify and distribute modified versions
- Link wirelog with proprietary software (with conditions)

For the full text of LGPL-3.0, see the file `COPYING.LGPL` or visit:
https://www.gnu.org/licenses/lgpl-3.0.html

### LGPL-3.0 Conditions Summary:
- Include a copy of the LGPL-3.0 license with your distribution
- Document any modifications to wirelog
- If you distribute applications using wirelog, you must provide:
  - Source code of wirelog modifications (if any)
  - Information on how to link the application with a modified version of wirelog
- Use of wirelog does not require you to open-source your own application

---

## 2. Commercial License

If you wish to use wirelog under a **proprietary/commercial license** (e.g., to avoid LGPL obligations or for closed-source distribution), please contact:

📧 **inquiry@cleverplant.com**

### Commercial License Inquiries:
- **License Type**: Proprietary/Commercial
- **Use Cases**:
  - Closed-source commercial applications
  - Proprietary extensions
  - Custom development agreements
  - OEM licensing
- **Support**: Custom support and maintenance agreements available

---

## 3. How to Choose

| Scenario | License |
|----------|---------|
| Open-source project | LGPL-3.0 |
| Proprietary application using wirelog as library | LGPL-3.0 ✓ (allowed) |
| Closed-source application | Commercial License required |
| Distribution requiring proprietary terms | Commercial License required |
| Custom/enterprise agreement | Commercial License (contact us) |

---

## 4. Attribution

When using wirelog under LGPL-3.0, please include this notice:

```
wirelog - Embedded-to-Enterprise Datalog Engine
Copyright (C) CleverPlant
Licensed under LGPL-3.0
https://github.com/semantic-reasoning/wirelog
```

---

## 5. Source Code Header

Each source file should include:

```c
/*
 * wirelog - Embedded-to-Enterprise Datalog Engine
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library.  If not, see
 * <https://www.gnu.org/licenses/lgpl-3.0.html>.
 */
```

---

## 6. Third-Party Licenses

wirelog uses the following third-party components:

| Component | License | Required |
|-----------|---------|----------|
| nanoarrow | Apache 2.0 | Optional (Phase 3+) |
| Differential Dataflow | MIT/Apache 2.0 | Phase 0-3 |
| Meson Build System | Apache 2.0 | Build-time only |

All third-party licenses are compatible with LGPL-3.0.

---

## 7. Legal Disclaimer

wirelog is provided "AS IS" without any warranty. See LGPL-3.0 for full disclaimer.

For questions about licensing:
- **Open Source (LGPL-3.0)**: Refer to https://www.gnu.org/licenses/lgpl-3.0.html
- **Commercial Licensing**: inquiry@cleverplant.com

---

**Last Updated**: 2026-02-22
